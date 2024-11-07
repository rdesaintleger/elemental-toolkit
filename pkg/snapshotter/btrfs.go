/*
Copyright © 2022 - 2024 SUSE LLC

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

package snapshotter

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/rancher/elemental-toolkit/v2/pkg/constants"
	"github.com/rancher/elemental-toolkit/v2/pkg/elemental"
	"github.com/rancher/elemental-toolkit/v2/pkg/types"
	"github.com/rancher/elemental-toolkit/v2/pkg/utils"
)

const (
	rootSubvol        = "@"
	snapshotsPath     = ".snapshots"
	snapshotPathTmpl  = ".snapshots/%d/snapshot"
	snapshotPathRegex = `.snapshots/(\d+)/snapshot`
	snapshotInfoPath  = ".snapshots/%d/info.xml"
	snapshotWorkDir   = "snapshot.workDir"
	installProgress   = "install-in-progress"
	updateProgress    = "update-in-progress"
)

func configTemplatesPaths() []string {
	return []string{
		"/etc/snapper/config-templates/default",
		"/usr/share/snapper/config-templates/default",
	}
}

var _ types.Snapshotter = (*Btrfs)(nil)

type subvolumeBackend interface {
	Probe(device string, mountpoint string) (stat backendStat, err error)
	InitBrfsPartition(rootDir string) error
	CreateNewSnapshot(rootDir string, snapshotsdDir string, baseID int) (*types.Snapshot, error)
	CommitSnapshot(rootDir string, snapshot *types.Snapshot) error
	ListSnapshots(rootDir string) (snapshotsList, error)
	DeleteSnapshot(rootDir string, id int) error
	SnapshotsCleanup(rootDir string) error
}

type snapshotsList struct {
	IDs      []int
	ActiveID int
}

type backendStat struct {
	ActiveID   int
	CurrentID  int
	RootDir    string
	StateMount string
}

type Btrfs struct {
	cfg              types.Config
	snapshotterCfg   types.SnapshotterConfig
	btrfsCfg         types.BtrfsConfig
	rootDir          string
	efiDir           string
	activeSnapshotID int
	bootloader       types.Bootloader
	backend          subvolumeBackend
}

// newBtrfsSnapshotter creates a new btrfs snapshotter vased on the given configuration and the given bootloader
func newBtrfsSnapshotter(cfg types.Config, snapCfg types.SnapshotterConfig, bootloader types.Bootloader) (types.Snapshotter, error) {
	if snapCfg.Type != constants.BtrfsSnapshotterType {
		msg := "invalid snapshotter type ('%s'), must be of '%s' type"
		cfg.Logger.Errorf(msg, snapCfg.Type, constants.BtrfsSnapshotterType)
		return nil, fmt.Errorf(msg, snapCfg.Type, constants.BtrfsSnapshotterType)
	}
	var btrfsCfg *types.BtrfsConfig
	var ok bool
	if snapCfg.Config == nil {
		btrfsCfg = types.NewBtrfsConfig()
	} else {
		btrfsCfg, ok = snapCfg.Config.(*types.BtrfsConfig)
		if !ok {
			msg := "failed casting BtrfsConfig type"
			cfg.Logger.Errorf(msg)
			return nil, fmt.Errorf("%s", msg)
		}
	}
	return &Btrfs{
		cfg: cfg, snapshotterCfg: snapCfg,
		btrfsCfg: *btrfsCfg, bootloader: bootloader,
		backend: NewSubvolumeBackend(&cfg, *btrfsCfg, snapCfg.MaxSnaps),
	}, nil
}

// NewSubvolumeBackend returns an instance of a subvolume backend
func NewSubvolumeBackend(cfg *types.Config, bCfg types.BtrfsConfig, maxSnaps int) subvolumeBackend {
	if bCfg.Snapper {
		return newSnapperBackend(cfg, maxSnaps)
	}
	return newBtrfsBackend(cfg, maxSnaps)
}

// InitSnapshotter initiates the snapshotter to the given root directory. This method includes the logic to create
// required subvolmes to handle snapshots as snapper does.
func (b *Btrfs) InitSnapshotter(state *types.Partition, efiDir string) error {
	var err error
	var ok bool

	b.cfg.Logger.Infof("Initiate btrfs snapshotter at %s", state.MountPoint)
	b.efiDir = efiDir

	b.cfg.Logger.Debug("Checking if essential subvolumes are already created")
	if ok, err = b.isInitiated(state); ok {
		if elemental.IsActiveMode(b.cfg) || elemental.IsPassiveMode(b.cfg) {
			return nil
		}
		b.cfg.Logger.Debug("Remount state partition at root subvolume")
	} else if err != nil {
		b.cfg.Logger.Errorf("failed loading initial snapshotter state: %s", err.Error())
		return err
	} else {
		b.cfg.Logger.Debug("Running initial btrfs configuration")
		// no snapshot callback here since no valid snapshot ID is available
		err = b.backend.InitBrfsPartition(state.MountPoint)
		if err != nil {
			b.cfg.Logger.Errorf("failed setting the btrfs partition for snapshots: %s", err.Error())
			return err
		}
	}

	return b.remountStatePartition(state)
}

// StartTransaction starts a transaction for this snapshotter instance and returns the work in progress snapshot object.
func (b *Btrfs) StartTransaction() (*types.Snapshot, error) {
	var newID int
	var err error
	var snapshot *types.Snapshot

	b.cfg.Logger.Info("Starting a btrfs snapshotter transaction")

	if b.rootDir == "" {
		b.cfg.Logger.Errorf("snapshotter should have been initalized before starting a transaction")
		return nil, fmt.Errorf("uninitialized snapshotter")
	}

	err = b.snapshotCallback(b.activeSnapshotID, func(rootDir string, snapshotsdDir string) error {
		snapshot, err = b.backend.CreateNewSnapshot(rootDir, snapshotsdDir, b.activeSnapshotID)
		return err
	})
	if err != nil {
		b.cfg.Logger.Errorf("failed creating new snapshot: %v", err)
		return nil, err
	}

	err = utils.MkdirAll(b.cfg.Fs, constants.WorkingImgDir, constants.DirPerm)
	if err != nil {
		b.cfg.Logger.Errorf("failed creating working tree directory: %s", constants.WorkingImgDir)
		return nil, err
	}

	err = b.cfg.Mounter.Mount(snapshot.WorkDir, constants.WorkingImgDir, "bind", []string{"bind"})
	if err != nil {
		_ = b.DeleteSnapshot(newID)
		return nil, err
	}
	snapshot.MountPoint = constants.WorkingImgDir
	snapshot.InProgress = true

	return snapshot, err
}

// CloseTransactionOnError is a destructor method to clean the given initated snapshot. Useful in case of an error once
// the transaction has already started.
func (b *Btrfs) CloseTransactionOnError(snapshot *types.Snapshot) (err error) {
	if snapshot.InProgress {
		_ = b.cfg.Mounter.Unmount(snapshot.MountPoint)
	}
	err = b.DeleteSnapshot(snapshot.ID)
	return err
}

// CloseTransaction closes the transaction for the given snapshot. This is the responsible to set
// the active btrfs subvolume
func (b *Btrfs) CloseTransaction(snapshot *types.Snapshot) (err error) {
	if !snapshot.InProgress {
		b.cfg.Logger.Debugf("No transaction to close for snapshot %d workdir", snapshot.ID)
		return fmt.Errorf("given snapshot is not in progress")
	}
	defer func() {
		if err != nil {
			_ = b.DeleteSnapshot(snapshot.ID)
		}
	}()
	b.cfg.Logger.Infof("Closing transaction for snapshot %d workdir", snapshot.ID)

	// Make sure snapshots mountpoint folder is part of the resulting snapshot image
	err = utils.MkdirAll(b.cfg.Fs, filepath.Join(snapshot.WorkDir, snapshotsPath), constants.DirPerm)
	if err != nil {
		b.cfg.Logger.Errorf("failed creating snapshots folder: %v", err)
		return err
	}

	b.cfg.Logger.Debugf("Unmount %s", snapshot.MountPoint)
	err = b.cfg.Mounter.Unmount(snapshot.MountPoint)
	if err != nil {
		b.cfg.Logger.Errorf("failed umounting snapshot %d workdir bind mount", snapshot.ID)
		return err
	}

	if snapshot.ID > 1 {
		// These steps are not required for the first snapshot (snapshot.ID = 1), in that
		// case snapshot.Path and snapshot.Workdir have the same value.
		err = utils.MirrorData(b.cfg.Logger, b.cfg.Runner, b.cfg.Fs, snapshot.WorkDir, snapshot.Path)
		if err != nil {
			b.cfg.Logger.Errorf("failed syncing working directory with snapshot directory")
			return err
		}

		err = b.cfg.Fs.RemoveAll(snapshot.WorkDir)
		if err != nil {
			b.cfg.Logger.Errorf("failed deleting snapshot's workdir '%s': %s", snapshot.WorkDir, err)
			return err
		}
	}

	extraBind := map[string]string{filepath.Join(b.rootDir, snapshotsPath): filepath.Join("/", snapshotsPath)}
	err = elemental.ApplySELinuxLabels(b.cfg, snapshot.Path, extraBind)
	if err != nil {
		b.cfg.Logger.Errorf("failed relabelling snapshot path: %s", snapshot.Path)
		return err
	}

	err = b.snapshotCallback(snapshot.ID, func(rootDir string, snapshotsdDir string) error {
		err = b.backend.CommitSnapshot(rootDir, snapshot)
		if err != nil {
			b.cfg.Logger.Errorf("failed relabelling snapshot path: %s", snapshot.Path)
			return err
		}

		// cleanup snapshots before setting bootloader otherwise deleted snapshots may show up in bootloader
		_ = b.backend.SnapshotsCleanup(rootDir)
		return nil
	})
	if err != nil {
		return err
	}

	_ = b.setBootloader(snapshot.ID)
	return nil
}

// DeleteSnapshot deletes the snapshot of the given ID. It cannot delete the current snapshot, if any.
func (b *Btrfs) DeleteSnapshot(id int) error {
	b.cfg.Logger.Infof("Deleting snapshot %d", id)

	snapshots, err := b.GetSnapshots()
	if err != nil {
		b.cfg.Logger.Errorf("failed listing available snapshots: %v", err)
		return err
	}
	if !slices.Contains(snapshots, id) {
		b.cfg.Logger.Debugf("snapshot %d not found, nothing has been deleted", id)
		return nil
	}

	return b.snapshotCallback(b.activeSnapshotID, func(rootDir string, snapshotsdDir string) error {
		return b.backend.DeleteSnapshot(rootDir, id)
	})
}

// GetSnapshots returns a list of the available snapshots IDs. It does not return any value if
// this Btrfs instance has not previously called InitSnapshotter.
func (b *Btrfs) GetSnapshots() (snapshots []int, err error) {
	if b.rootDir == "" {
		return nil, fmt.Errorf("snapshotter not initiated yet, run 'InitSnapshotter' before calling this method")
	}

	return b.getSnapshotsInternal(b.activeSnapshotID)
}

func (b *Btrfs) getSnapshotsInternal(snapshotID int) (snapshots []int, err error) {
	var snapList snapshotsList

	if b.rootDir == "" {
		return nil, fmt.Errorf("snapshotter not initiated yet, run 'InitSnapshotter' before calling this method")
	}

	if snapshotID > 0 {
		err = b.snapshotCallback(snapshotID, func(rootDir string, snapshotsdDir string) error {
			snapList, err = b.backend.ListSnapshots(rootDir)
			if err != nil {
				return err
			}
			b.activeSnapshotID = snapList.ActiveID
			return nil
		})
		if err != nil {
			return nil, err
		}
		return snapList.IDs, err
	}

	return []int{}, err
}

// SnapshotImageToSource converts the given snapshot into an ImageSource. This is useful to deploy a system
// from a given snapshot, for instance setting the recovery image from a snapshot.
func (b *Btrfs) SnapshotToImageSource(snap *types.Snapshot) (*types.ImageSource, error) {
	ok, err := utils.Exists(b.cfg.Fs, snap.Path)
	if err != nil || !ok {
		msg := fmt.Sprintf("snapshot path does not exist: %s.", snap.Path)
		b.cfg.Logger.Errorf(msg)
		if err == nil {
			err = fmt.Errorf("%s", msg)
		}
		return nil, err
	}
	return types.NewDirSrc(snap.Path), nil
}

// isInitiated checks if the given state partition has already the default
// subvolumes structure. It also parses and updates some additional parameters
// such as the state partition mountpoint and the active snapshot if any
func (b *Btrfs) isInitiated(state *types.Partition) (bool, error) {
	if b.activeSnapshotID > 0 {
		return true, nil
	}

	if b.rootDir != "" {
		return false, nil
	}

	// Probe is called without callback helper function since no valid snapshot id is available
	bStat, err := b.backend.Probe(state.Path, state.MountPoint)
	if err != nil {
		return false, err
	}
	b.activeSnapshotID = bStat.ActiveID
	b.rootDir = bStat.RootDir
	state.MountPoint = bStat.StateMount
	return bStat.ActiveID > 0, nil
}

// getPassiveSnapshots returns a list of the available snapshots
// excluding the acitve snapshot.
func (b *Btrfs) getPassiveSnapshots(snapshotID int) ([]int, error) {
	passives := []int{}

	snapshots, err := b.getSnapshotsInternal(snapshotID)
	if err != nil {
		return nil, err
	}
	for _, snapshot := range snapshots {
		if snapshot != snapshotID {
			passives = append(passives, snapshot)
		}
	}

	return passives, nil
}

// setBootloader sets the bootloader variables to update new passives
func (b *Btrfs) setBootloader(snapshotID int) error {
	var passives, fallbacks []string

	b.cfg.Logger.Infof("Setting bootloader with current passive snapshots")
	ids, err := b.getPassiveSnapshots(snapshotID)
	if err != nil {
		b.cfg.Logger.Warnf("failed getting current passive snapshots: %v", err)
		return err
	}

	for i := len(ids) - 1; i >= 0; i-- {
		passives = append(passives, strconv.Itoa(ids[i]))
	}

	// We count first is active, then all passives and finally the recovery
	for i := 0; i <= len(ids)+1; i++ {
		fallbacks = append(fallbacks, strconv.Itoa(i))
	}
	snapsList := strings.Join(passives, " ")
	fallbackList := strings.Join(fallbacks, " ")
	envFile := filepath.Join(b.efiDir, constants.GrubOEMEnv)

	envs := map[string]string{
		constants.GrubFallback:         fallbackList,
		constants.GrubPassiveSnapshots: snapsList,
		constants.GrubActiveSnapshot:   strconv.Itoa(snapshotID),
		"snapshotter":                  constants.BtrfsSnapshotterType,
	}

	err = b.bootloader.SetPersistentVariables(envFile, envs)
	if err != nil {
		b.cfg.Logger.Warnf("failed setting bootloader environment file %s: %v", envFile, err)
		return err
	}

	return err
}

// remountStatePartition umounts and mounts again the state partition with RW rights and
// it also mounts the snapshots subvolume under the active snapshot root tree.
func (b *Btrfs) remountStatePartition(state *types.Partition) error {
	b.cfg.Logger.Debugf("Umount %s", state.MountPoint)
	err := b.cfg.Mounter.Unmount(state.MountPoint)
	if err != nil {
		b.cfg.Logger.Errorf("failed unmounting %s: %v", state.MountPoint, err)
		return err
	}

	b.cfg.Logger.Debugf("Remount root '%s' on top level subvolume '%s'", state.MountPoint, rootSubvol)
	err = b.cfg.Mounter.Mount(state.Path, state.MountPoint, "btrfs", []string{"rw", fmt.Sprintf("subvol=%s", rootSubvol)})
	if err != nil {
		b.cfg.Logger.Errorf("failed mounting subvolume %s at %s", rootSubvol, state.MountPoint)
		return err
	}
	return err
}

func (b *Btrfs) snapshotCallback(snapshotID int, callback func(rootDir string, snapshotsdDir string) error) (err error) {
	rootDir := b.rootDir
	snapshotsdDir := filepath.Join(rootDir, snapshotsPath)

	if snapshotID > 0 {
		target := filepath.Join(rootDir, fmt.Sprintf(snapshotPathTmpl, snapshotID), snapshotsPath)
		if rootDir != "/" {
			// Check if snapshots subvolume is mounted
			rootDir = filepath.Dir(target)
			if notMnt, _ := b.cfg.Mounter.IsLikelyNotMountPoint(target); notMnt {
				b.cfg.Logger.Debugf("Mount snapshots subvolume in snapshot %d", snapshotID)
				err = b.cfg.Mounter.Mount(snapshotsdDir, target, "bind", []string{"bind"})
				if err != nil {
					return err
				}
				defer func() {
					b.cfg.Logger.Debugf("Unmount snapshots subvolume in snapshot %d", snapshotID)
					nErr := b.cfg.Mounter.Unmount(target)

					if err == nil && nErr != nil {
						err = nErr
					}
				}()
			}
		}
	}

	return callback(rootDir, snapshotsdDir)
}
