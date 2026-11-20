/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package basic

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	velerov1api "github.com/velero-io/velero/pkg/apis/velero/v1"
	veleroexec "github.com/velero-io/velero/pkg/util/exec"
	. "github.com/velero-io/velero/test"
	. "github.com/velero-io/velero/test/e2e/test"
	"github.com/velero-io/velero/test/util/common"
	. "github.com/velero-io/velero/test/util/k8s"
	. "github.com/velero-io/velero/test/util/velero"
)

// The csi-driver-host-path plugin that hack/install-csi-hostpath.sh installs
// keeps every volume and snapshot it manages in a JSON state file. Reading it
// is the only way to see what exists on the storage side: the Kubernetes
// objects can be gone while the snapshots they referenced are not.
const (
	hostpathPluginNamespace = "default"
	hostpathPluginPod       = "csi-hostpathplugin-0"
	hostpathPluginContainer = "hostpath"
	hostpathStateFile       = "/csi-data-dir/state.json"

	vgsCleanupGroup = "cleanup"
	// snapshotCleanupTimeout bounds how long the storage side is given to
	// catch up after Velero reports that it is done.
	snapshotCleanupTimeout = 2 * time.Minute
)

var CSIVolumeGroupSnapshotCleanupTest func() = TestFunc(&CSIVolumeGroupSnapshotCleanup{
	BackupVolumeInfo: BackupVolumeInfo{
		SnapshotVolumes: true,
		TestCase: TestCase{
			CaseBaseName: "csi-vgs-cleanup",
			TestMsg: &TestMSG{
				Desc: "Test that deleting a CSI backup removes the storage snapshots of a VolumeGroupSnapshot.",
				Text: "After the backup is deleted, no storage snapshot of a grouped or an ungrouped volume should be left.",
			},
		},
	},
})

var CSIVolumeGroupSnapshotDataMoverCleanupTest func() = TestFunc(&CSIVolumeGroupSnapshotCleanup{
	BackupVolumeInfo: BackupVolumeInfo{
		SnapshotVolumes:  true,
		SnapshotMoveData: true,
		TestCase: TestCase{
			CaseBaseName: "csi-vgs-dm-cleanup",
			TestMsg: &TestMSG{
				Desc: "Test that a CSI data mover backup removes the storage snapshots of a VolumeGroupSnapshot.",
				Text: "After the data is moved, no storage snapshot of a grouped or an ungrouped volume should be left.",
			},
		},
	},
})

// CSIVolumeGroupSnapshotCleanup backs up two grouped PVCs and one ungrouped
// PVC, and checks on the storage side that their snapshots are removed when
// Velero is done with them: when the backup is deleted for a CSI snapshot
// backup, and when the backup completes for a data mover backup. The
// ungrouped PVC is the control: it shows the check itself works.
type CSIVolumeGroupSnapshotCleanup struct {
	BackupVolumeInfo

	// volumeHandles maps each PVC name to the CSI volume handle of its PV.
	volumeHandles map[string]string
	groupedPVCs   []string
	ungroupedPVC  string
}

type hostpathSnapshot struct {
	Id              string
	VolID           string
	GroupSnapshotID string
}

type hostpathGroupSnapshot struct {
	Id          string
	SnapshotIDs []string
}

type hostpathState struct {
	Snapshots      []hostpathSnapshot
	GroupSnapshots []hostpathGroupSnapshot
}

func (c *CSIVolumeGroupSnapshotCleanup) Start() error {
	if c.VeleroCfg.CloudProvider != Kind {
		Skip(fmt.Sprintf("%s reads the csi-driver-host-path state, which only the kind setup installs", c.CaseBaseName))
	}
	return c.BackupVolumeInfo.Start()
}

func (c *CSIVolumeGroupSnapshotCleanup) CreateResources() error {
	labels := map[string]string{
		"volume-info": "true",
	}
	if c.VeleroCfg.WorkerOS == common.WorkerOSWindows {
		labels["pod-security.kubernetes.io/enforce"] = "privileged"
		labels["pod-security.kubernetes.io/enforce-version"] = "latest"
	}

	namespace := c.CaseBaseName
	fmt.Printf("Creating namespace %s ...\n", namespace)
	if err := CreateNamespaceWithLabel(c.Ctx, c.Client, namespace, labels); err != nil {
		return errors.Wrapf(err, "failed to create namespace %s", namespace)
	}

	c.groupedPVCs = []string{"vgs-cleanup-pvc-0", "vgs-cleanup-pvc-1"}
	c.ungroupedPVC = "vgs-cleanup-pvc-single"

	var vols []*corev1api.Volume
	for i, pvcName := range append(append([]string{}, c.groupedPVCs...), c.ungroupedPVC) {
		pvcBuilder := NewPVC(namespace, pvcName).WithStorageClass(StorageClassName)
		if pvcName != c.ungroupedPVC {
			pvcBuilder = pvcBuilder.WithLabels(map[string]string{velerov1api.DefaultVGSLabelKey: vgsCleanupGroup})
		}
		fmt.Printf("Creating PVC %s in namespace %s ...\n", pvcName, namespace)
		if err := CreatePvc(c.Client, pvcBuilder); err != nil {
			return errors.Wrapf(err, "failed to create PVC %s", pvcName)
		}
		vols = append(vols, CreateVolumes(pvcName, []string{fmt.Sprintf("vgs-cleanup-pv-%d", i)})...)
	}

	// The PVCs only bind once something mounts them.
	deployment := NewDeployment(
		c.CaseBaseName,
		namespace,
		1,
		labels,
		c.VeleroCfg.ImageRegistryProxy,
		c.VeleroCfg.WorkerOS,
	).WithVolume(vols).Result()
	deployment, err := CreateDeployment(c.Client.ClientGo, namespace, deployment)
	if err != nil {
		return errors.Wrapf(err, "failed to create deployment in namespace %s", namespace)
	}
	if err := WaitForReadyDeployment(c.Client.ClientGo, namespace, deployment.Name); err != nil {
		return errors.Wrapf(err, "failed to wait for the deployment in namespace %s", namespace)
	}

	c.volumeHandles = map[string]string{}
	for _, pvcName := range append(append([]string{}, c.groupedPVCs...), c.ungroupedPVC) {
		pvc, err := GetPVC(c.Ctx, c.Client, namespace, pvcName)
		if err != nil {
			return errors.Wrapf(err, "failed to get PVC %s", pvcName)
		}
		pv, err := c.Client.ClientGo.CoreV1().PersistentVolumes().Get(c.Ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to get PV %s of PVC %s", pvc.Spec.VolumeName, pvcName)
		}
		if pv.Spec.CSI == nil {
			return errors.Errorf("PV %s of PVC %s is not a CSI volume", pv.Name, pvcName)
		}
		c.volumeHandles[pvcName] = pv.Spec.CSI.VolumeHandle
		fmt.Printf("PVC %s: volume handle %s\n", pvcName, pv.Spec.CSI.VolumeHandle)
	}
	return nil
}

// Destroy keeps the workload. The check is about the snapshots, and keeping
// the source volumes means their removal cannot be mistaken for a cleanup.
func (c *CSIVolumeGroupSnapshotCleanup) Destroy() error {
	return nil
}

// Restore is not part of this case.
func (c *CSIVolumeGroupSnapshotCleanup) Restore() error {
	return nil
}

func (c *CSIVolumeGroupSnapshotCleanup) Verify() error {
	state, err := readHostpathState(c.Ctx)
	Expect(err).ToNot(HaveOccurred(), "failed to read the csi-driver-host-path state")
	fmt.Println("Storage snapshots after the backup completed:")
	c.printSnapshots(state)

	if c.SnapshotMoveData {
		// A data mover backup is done with its snapshots once the data is
		// moved. Report what is left at that point, then delete the backup and
		// check again: whatever is still there then is never removed.
		By("Waiting for the snapshots to be removed now that the data has been moved", func() {
			if leftovers := c.waitForNoSnapshots(); len(leftovers) > 0 {
				fmt.Printf("Left after the data was moved: %s\n", strings.Join(leftovers, "; "))
			}
		})
		By(fmt.Sprintf("Deleting backup %s", c.BackupName), func() {
			Expect(DeleteBackup(c.Ctx, c.BackupName, &c.VeleroCfg)).To(Succeed())
			Expect(WaitBackupDeleted(c.Ctx, c.BackupName, 5*time.Minute, &c.VeleroCfg)).To(Succeed())
		})
		By("Waiting for the snapshots to be removed now that the backup is deleted", func() {
			c.expectNoSnapshotsLeft()
		})
		return nil
	}

	// A CSI snapshot backup keeps its snapshots until the backup is deleted.
	// Check they were all taken, and that the grouped ones came from one group
	// snapshot, so the check below is not passing for the wrong reason.
	for _, pvcName := range c.groupedPVCs {
		snaps := snapshotsOfVolume(state, c.volumeHandles[pvcName])
		Expect(snaps).To(HaveLen(1), fmt.Sprintf("expected one storage snapshot of grouped PVC %s", pvcName))
		Expect(snaps[0].GroupSnapshotID).NotTo(BeEmpty(),
			fmt.Sprintf("the snapshot of PVC %s was not taken as part of a group snapshot", pvcName))
	}
	Expect(snapshotsOfVolume(state, c.volumeHandles[c.ungroupedPVC])).To(HaveLen(1),
		fmt.Sprintf("expected one storage snapshot of ungrouped PVC %s", c.ungroupedPVC))

	By(fmt.Sprintf("Deleting backup %s", c.BackupName), func() {
		Expect(DeleteBackup(c.Ctx, c.BackupName, &c.VeleroCfg)).To(Succeed())
		Expect(WaitBackupDeleted(c.Ctx, c.BackupName, 5*time.Minute, &c.VeleroCfg)).To(Succeed())
	})

	By("Waiting for the snapshots to be removed now that the backup is deleted", func() {
		c.expectNoSnapshotsLeft()
	})
	return nil
}

// expectNoSnapshotsLeft fails with the storage snapshots that are not
// removed in time. Each one names its PVC and whether it is grouped, so a
// failure shows whether only group members leaked.
func (c *CSIVolumeGroupSnapshotCleanup) expectNoSnapshotsLeft() {
	leftovers := c.waitForNoSnapshots()
	Expect(leftovers).To(BeEmpty(), fmt.Sprintf(
		"storage snapshots were left behind: %s", strings.Join(leftovers, "; ")))
}

// waitForNoSnapshots waits for every storage snapshot of the case's volumes to
// be removed, and returns and prints the ones still there when it gives up.
func (c *CSIVolumeGroupSnapshotCleanup) waitForNoSnapshots() []string {
	err := wait.PollUntilContextTimeout(c.Ctx, 5*time.Second, snapshotCleanupTimeout, true,
		func(ctx context.Context) (bool, error) {
			state, err := readHostpathState(ctx)
			if err != nil {
				// A read cancelled because the poll timed out is not a read failure.
				if ctx.Err() == nil {
					fmt.Printf("Failed to read the csi-driver-host-path state, retrying: %v\n", err)
				}
				return false, nil
			}
			return len(c.leftovers(state)) == 0, nil
		})
	if err == nil {
		fmt.Println("No storage snapshot of the case's volumes is left.")
		return nil
	}

	// Read once more outside the poll: its context has expired by now.
	state, readErr := readHostpathState(c.Ctx)
	Expect(readErr).ToNot(HaveOccurred(), "failed to read the csi-driver-host-path state")

	fmt.Printf("Storage snapshots still present after %s:\n", snapshotCleanupTimeout)
	c.printSnapshots(state)
	c.printSnapshotContents(state)
	return c.leftovers(state)
}

// leftovers describes every storage snapshot that still exists for one of the
// case's volumes.
func (c *CSIVolumeGroupSnapshotCleanup) leftovers(state *hostpathState) []string {
	var out []string
	for _, pvcName := range c.pvcNames() {
		for _, snap := range snapshotsOfVolume(state, c.volumeHandles[pvcName]) {
			kind := "ungrouped"
			if pvcName != c.ungroupedPVC {
				kind = "grouped"
			}
			out = append(out, fmt.Sprintf("%s PVC %s: snapshot %s (group snapshot %q)",
				kind, pvcName, snap.Id, snap.GroupSnapshotID))
		}
	}
	return out
}

func (c *CSIVolumeGroupSnapshotCleanup) pvcNames() []string {
	return append(append([]string{}, c.groupedPVCs...), c.ungroupedPVC)
}

func (c *CSIVolumeGroupSnapshotCleanup) printSnapshots(state *hostpathState) {
	groups := map[string]bool{}
	for _, pvcName := range c.pvcNames() {
		snaps := snapshotsOfVolume(state, c.volumeHandles[pvcName])
		if len(snaps) == 0 {
			fmt.Printf("  PVC %s: no snapshot\n", pvcName)
		}
		for _, snap := range snaps {
			fmt.Printf("  PVC %s: snapshot %s, group snapshot %q\n", pvcName, snap.Id, snap.GroupSnapshotID)
			if snap.GroupSnapshotID != "" {
				groups[snap.GroupSnapshotID] = true
			}
		}
	}
	for _, group := range state.GroupSnapshots {
		if groups[group.Id] {
			fmt.Printf("  group snapshot %s: snapshots %v\n", group.Id, group.SnapshotIDs)
		}
	}
}

// printSnapshotContents lists the VolumeSnapshotContents and
// VolumeGroupSnapshotContents that still reference a leftover snapshot, to
// show whether the Kubernetes objects leaked as well or only the storage side.
func (c *CSIVolumeGroupSnapshotCleanup) printSnapshotContents(state *hostpathState) {
	ids := map[string]bool{}
	for _, pvcName := range c.pvcNames() {
		for _, snap := range snapshotsOfVolume(state, c.volumeHandles[pvcName]) {
			ids[snap.Id] = true
			if snap.GroupSnapshotID != "" {
				ids[snap.GroupSnapshotID] = true
			}
		}
	}

	for _, resource := range []string{"volumesnapshotcontents", "volumegroupsnapshotcontents"} {
		out, err := kubectlGetJSON(c.Ctx, resource)
		if err != nil {
			fmt.Printf("  failed to list %s: %v\n", resource, err)
			continue
		}
		var list struct {
			Items []struct {
				Metadata struct {
					Name              string     `json:"name"`
					DeletionTimestamp *time.Time `json:"deletionTimestamp"`
				} `json:"metadata"`
				Spec struct {
					DeletionPolicy string `json:"deletionPolicy"`
				} `json:"spec"`
				Status *struct {
					SnapshotHandle            *string `json:"snapshotHandle"`
					VolumeGroupSnapshotHandle *string `json:"volumeGroupSnapshotHandle"`
				} `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal([]byte(out), &list); err != nil {
			fmt.Printf("  failed to decode %s: %v\n", resource, err)
			continue
		}
		found := false
		for _, item := range list.Items {
			if item.Status == nil {
				continue
			}
			handle := ""
			if item.Status.SnapshotHandle != nil {
				handle = *item.Status.SnapshotHandle
			} else if item.Status.VolumeGroupSnapshotHandle != nil {
				handle = *item.Status.VolumeGroupSnapshotHandle
			}
			if !ids[handle] {
				continue
			}
			found = true
			fmt.Printf("  %s %s: handle %s, deletionPolicy %s, being deleted %t\n",
				resource, item.Metadata.Name, handle, item.Spec.DeletionPolicy, item.Metadata.DeletionTimestamp != nil)
		}
		if !found {
			fmt.Printf("  no %s references a leftover snapshot\n", resource)
		}
	}
}

func snapshotsOfVolume(state *hostpathState, volumeHandle string) []hostpathSnapshot {
	var out []hostpathSnapshot
	for _, snap := range state.Snapshots {
		if snap.VolID == volumeHandle {
			out = append(out, snap)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out
}

func readHostpathState(ctx context.Context) (*hostpathState, error) {
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", hostpathPluginNamespace,
		"-c", hostpathPluginContainer, hostpathPluginPod, "--", "cat", hostpathStateFile)
	stdout, stderr, err := veleroexec.RunCommand(cmd)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read %s from %s: %s", hostpathStateFile, hostpathPluginPod, stderr)
	}
	state := &hostpathState{}
	if err := json.Unmarshal([]byte(stdout), state); err != nil {
		return nil, errors.Wrapf(err, "failed to decode %s", hostpathStateFile)
	}
	return state, nil
}

func kubectlGetJSON(ctx context.Context, resource string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", "get", resource, "-o", "json")
	stdout, stderr, err := veleroexec.RunCommand(cmd)
	if err != nil {
		return "", errors.Wrapf(err, "kubectl get %s: %s", resource, stderr)
	}
	return stdout, nil
}
