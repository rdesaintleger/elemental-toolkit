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
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/rancher/elemental-toolkit/v2/pkg/constants"
	"github.com/rancher/elemental-toolkit/v2/pkg/elemental"
	"github.com/rancher/elemental-toolkit/v2/pkg/types"
	"github.com/rancher/elemental-toolkit/v2/pkg/utils"
)

const (
	topSubvolID          = 5
	rootSubvol           = "@"
	snapshotsPath        = ".snapshots"
	snapshotPathTmpl     = ".snapshots/%d/snapshot"
	snapshotPathRegex    = `.snapshots/(\d+)/snapshot`
	snapshotWorkDir      = "snapshot.workDir"
	snapperBootstrapPath = ".bootstrap"
	snapperRootConfig    = "/etc/snapper/configs/root"
	snapperSysconfig     = "/etc/sysconfig/snapper"
	snapperDefaultconfig = "/etc/default/snapper"
)

var _ types.Snapshotter = (*Btrfs)(nil)

type Btrfs struct {
	cfg              types.Config
	snapshotterCfg   types.SnapshotterConfig
	btrfsCfg         types.BtrfsConfig
	rootDir          string
	snapshotsDir     string
	stateDir         string
	efiDir           string
	activeSnapshotID int
	bootloader       types.Bootloader
}

type btrfsSubvol struct {
	path string
	id   int
}

type btrfsSubvolList []btrfsSubvol

// NewLoopDeviceSnapshotter creates a new loop device snapshotter vased on the given configuration and the given bootloader
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
		if !btrfsCfg.DisableSnapper && btrfsCfg.DisableDefaultSubVolume {
			msg := "requested snapshotter configuration is invalid"
			cfg.Logger.Errorf(msg)
			return nil, fmt.Errorf("%s", msg)
		}
	}
	return &Btrfs{
		cfg: cfg, snapshotterCfg: snapCfg, btrfsCfg: *btrfsCfg,
		bootloader: bootloader,
	}, nil
}

func (b *Btrfs) InitSnapshotter(state *types.Partition, efiDir string) error {
	var err error
	var ok bool

	b.cfg.Logger.Infof("Initiate btrfs snapshotter at %s", state.MountPoint)
	b.stateDir = state.MountPoint
	b.efiDir = efiDir

	b.cfg.Logger.Debug("Checking if essential subvolumes are already created")
	if ok, err = b.isInitiated(state.MountPoint); ok {
		if elemental.IsActiveMode(b.cfg) || elemental.IsPassiveMode(b.cfg) {
			// if in active or passive mode, assume that root, state and snapshots are already mounted
			return b.configureMountPointAndRootDir(state)
		}
	} else if err != nil {
		b.cfg.Logger.Errorf("failed loading initial snapshotter state: %v")
		return err
	} else {
		b.cfg.Logger.Debug("Running initial btrfs configuration")
		err = b.setBtrfsForFirstTime(state)
		if err != nil {
			return err
		}
	}

	b.cfg.Logger.Debug("Remount state partition at root subvolume")
	return b.remountStatePartition(state)
}

func (b *Btrfs) StartTransaction() (*types.Snapshot, error) {
	var newID int
	var err error
	var workingDir, path string
	snapshot := &types.Snapshot{}

	b.cfg.Logger.Info("Starting a btrfs snapshotter transaction")

	if b.stateDir == "" || b.snapshotsDir == "" {
		b.cfg.Logger.Errorf("Snapshotter should have been initalized before starting a transaction")
		return nil, fmt.Errorf("uninitialized snapshotter")
	}

	if b.activeSnapshotID > 0 {
		b.cfg.Logger.Infof("Creating a new snapshot from %d", b.activeSnapshotID)
		if !b.btrfsCfg.DisableSnapper {
			args := []string{
				"create", "--from", strconv.Itoa(b.activeSnapshotID),
				"--read-write",
				"--print-number",
				"--description", fmt.Sprintf("Update for snapshot %d", b.activeSnapshotID),
				"--cleanup-algorithm", "number",
				"--userdata", "update-in-progress=yes",
			}
			cmdOut, err := b.runCurrentSnapper(args...)
			if err != nil {
				b.cfg.Logger.Errorf("snapper failed to create a new snapshot: %v", err)
				return nil, err
			}
			newID, err = strconv.Atoi(strings.TrimSpace(string(cmdOut)))
			if err != nil {
				b.cfg.Logger.Errorf("failed parsing new snapshot ID")
				return nil, err
			}
		} else {
			ids, err := b.GetSnapshots()
			if err != nil {
				b.cfg.Logger.Errorf("unable to get btrfs snapshots")
				return nil, err
			}

			// minimum ID
			newID = 1

			for _, id := range ids {
				// search for next ID to be used
				newID = max(id+1, newID)
			}
		}

		path = filepath.Join(filepath.Dir(b.snapshotsDir), fmt.Sprintf(snapshotPathTmpl, newID))
		workingDir = filepath.Join(filepath.Dir(path), snapshotWorkDir)
		err = utils.MkdirAll(b.cfg.Fs, workingDir, constants.DirPerm)
		if err != nil {
			b.cfg.Logger.Errorf("failed creating the snapshot working directory: %v", err)
			_ = b.DeleteSnapshot(newID)
			return nil, err
		}

		if b.btrfsCfg.DisableSnapper {
			source := filepath.Join(filepath.Dir(b.snapshotsDir), fmt.Sprintf(snapshotPathTmpl, b.activeSnapshotID))
			cmdOut, err := b.cfg.Runner.Run("btrfs", "subvolume", "snapshot", "-i", "1/0", source, path)
			if err != nil {
				b.cfg.Logger.Errorf("failed creating snapshot volume: %s", string(cmdOut))
				_ = b.DeleteSnapshot(newID)
				return nil, err
			}
		}
	} else {
		b.cfg.Logger.Info("Bootstraping first root filesystem")

		// assume ID of snapshot will be '1'
		newID = 1
		path = filepath.Join(filepath.Dir(b.snapshotsDir), fmt.Sprintf(snapshotPathTmpl, newID))

		if !b.btrfsCfg.DisableSnapper {
			workingDir = filepath.Join(b.stateDir, snapperBootstrapPath)

			// make all parent directories but not target as it will be a subvolume
			err = utils.MkdirAll(b.cfg.Fs, filepath.Dir(workingDir), constants.DirPerm)
			if err != nil {
				return nil, err
			}
			cmdOut, err := b.cfg.Runner.Run(
				"btrfs", "subvolume", "create", "-i", "1/0",
				workingDir,
			)
			if err != nil {
				b.cfg.Logger.Errorf("failed creating first snapshot volume: %s", string(cmdOut))
				return nil, err
			}
		} else {
			workingDir = filepath.Join(filepath.Dir(path), snapshotWorkDir)

			// create snapshot working dir
			err = utils.MkdirAll(b.cfg.Fs, workingDir, constants.DirPerm)
			if err != nil {
				b.cfg.Logger.Errorf("failed creating the snapshot working directory: %v", err)
				_ = b.DeleteSnapshot(newID)
				return nil, err
			}

			// create target subvolume
			cmdOut, err := b.cfg.Runner.Run("btrfs", "subvolume", "create", "-i", "1/0", path)
			if err != nil {
				b.cfg.Logger.Errorf("failed creating first snapshot volume: %s", string(cmdOut))
				_ = b.DeleteSnapshot(newID)
				return nil, err
			}
		}
	}

	err = utils.MkdirAll(b.cfg.Fs, constants.WorkingImgDir, constants.DirPerm)
	if err != nil {
		b.cfg.Logger.Errorf("failed creating working tree directory: %s", constants.WorkingImgDir)
		_ = b.DeleteSnapshot(newID)
		return nil, err
	}

	err = b.cfg.Mounter.Mount(workingDir, constants.WorkingImgDir, "bind", []string{"bind"})
	if err != nil {
		_ = b.DeleteSnapshot(newID)
		return nil, err
	}
	snapshot.MountPoint = constants.WorkingImgDir
	snapshot.ID = newID
	snapshot.InProgress = true
	snapshot.WorkDir = workingDir
	snapshot.Path = path

	return snapshot, err
}

func (b *Btrfs) CloseTransactionOnError(snapshot *types.Snapshot) (err error) {
	if snapshot.InProgress {
		err = b.cfg.Mounter.Unmount(snapshot.MountPoint)

		if err != nil {
			b.cfg.Logger.Errorf("unable to unmount snapshot mountpoint %s: %v", snapshot.MountPoint, err)
		}
	}

	return b.DeleteSnapshot(snapshot.ID)
}

func (b *Btrfs) syncSnapshotWorkdir(snapshot *types.Snapshot) (err error) {
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

	snapshot.WorkDir = snapshot.Path
	return nil
}

func (b *Btrfs) CloseTransaction(snapshot *types.Snapshot) (err error) {
	var cmdOut []byte

	if !snapshot.InProgress {
		b.cfg.Logger.Debugf("No transaction to close for snapshot %d workdir", snapshot.ID)
		return fmt.Errorf("given snapshot is not in progress")
	}
	b.cfg.Logger.Infof("Closing transaction for snapshot %d workdir", snapshot.ID)

	snapshotsBind := map[string]string{b.snapshotsDir: filepath.Join("/", snapshotsPath)}

	b.cfg.Logger.Debugf("Unmount %s", snapshot.MountPoint)
	err = b.cfg.Mounter.Unmount(snapshot.MountPoint)
	if err != nil {
		b.cfg.Logger.Errorf("failed umounting snapshot %d workdir bind mount", snapshot.ID)
		return err
	}

	if !b.btrfsCfg.DisableSnapper {
		// when bootstraping snapper, snapshot is not created yet
		if ok, err := utils.Exists(b.cfg.Fs, snapshot.Path); !ok {
			// Configure snapper
			err = b.configureSnapshotSnapper(snapshot)
			if err != nil {
				b.cfg.Logger.Errorf("failed configuring snapper: %v", err)
				return err
			}
			cmdOut, err := b.runSnapshotSnapper(snapshot.WorkDir, snapshotsBind,
				"--config", "root", "create",
				"--read-write",
				"--cleanup-algorithm", "number",
				"--description", "first root filesystem",
				"--userdata", "update-in-progress=no")
			if err != nil {
				b.cfg.Logger.Errorf("unable to create initial snapshot: %v", err)
				b.cfg.Logger.Debugf("snapper output: %s", string(cmdOut))
				return err
			}

			// we now have the first snapshot. delete bootstrap volume
			_, err = b.deleteBootstrapSubvolume()
			if err != nil {
				b.cfg.Logger.Errorf("can't delete bootstrap subvolume: %v", err)
				return err
			}
			snapshot.WorkDir = snapshot.Path
		} else if err != nil {
			b.cfg.Logger.Errorf("unable to stat snapshot %d workdir", snapshot.ID)
			return err
		} else {
			b.syncSnapshotWorkdir(snapshot)

			// Configure snapper after sync (otherwise, snapper reports that workdir is not a volume)
			err = b.configureSnapshotSnapper(snapshot)
			if err != nil {
				b.cfg.Logger.Errorf("failed configuring snapper: %v", err)
				return err
			}

			args := []string{"modify", "--userdata", "update-in-progress=no", strconv.Itoa(snapshot.ID)}
			cmdOut, err := b.runCurrentSnapper(args...)
			if err != nil {
				b.cfg.Logger.Errorf("failed updating snapshot user data %d: %s", snapshot.ID, string(cmdOut))
				return err
			}
		}
	} else {
		b.syncSnapshotWorkdir(snapshot)
	}

	// Make sure snapshots mountpoint folder is part of the resulting snapshot image
	err = utils.MkdirAll(b.cfg.Fs, filepath.Join(snapshot.Path, snapshotsPath), constants.DirPerm)
	if err != nil {
		b.cfg.Logger.Errorf("failed creating snapshots folder: %v", err)
		return err
	}

	err = elemental.ApplySELinuxLabels(b.cfg, snapshot.Path, snapshotsBind)
	if err != nil {
		b.cfg.Logger.Errorf("failed relabelling snapshot path: %s", snapshot.Path)
		return err
	}

	// sets the snapshot readonly
	cmdOut, err = b.cfg.Runner.Run("btrfs", "property", "set", snapshot.Path, "ro", "true")
	if err != nil {
		b.cfg.Logger.Errorf("failed setting read only property to snapshot %d: %s", snapshot.ID, string(cmdOut))
		return err
	}

	defaultSubvolID := topSubvolID
	if !b.btrfsCfg.DisableDefaultSubVolume {
		defaultSubvolID, err = b.findSubvolumeByPath(snapshot.Path)
		if err != nil {
			b.cfg.Logger.Error("failed finding subvolume")
			return err
		}
	} else {
		// when not using default subvolume, a symlink is used in the state directory
		// to identify active snapshot (loopdevice snapshotter use same feature)
		activeSnap := filepath.Join(b.stateDir, constants.ActiveSnapshot)
		linkDst := fmt.Sprintf(snapshotPathTmpl, snapshot.ID)
		b.cfg.Logger.Debugf("creating symlink %s to %s", activeSnap, linkDst)
		_ = b.cfg.Fs.Remove(activeSnap)
		err = b.cfg.Fs.Symlink(linkDst, activeSnap)
		if err != nil {
			b.cfg.Logger.Errorf("failed default snapshot image for snapshot %d: %v", snapshot.ID, err)
			sErr := b.cfg.Fs.Symlink(fmt.Sprintf(snapshotPathTmpl, b.activeSnapshotID), activeSnap)
			if sErr != nil {
				b.cfg.Logger.Warnf("could not restore previous active link")
			}
			return err
		}
	}

	// ensure default subvolume is always up to date
	cmdOut, err = b.cfg.Runner.Run("btrfs", "subvolume", "set-default", strconv.Itoa(defaultSubvolID), snapshot.Path)
	if err != nil {
		b.cfg.Logger.Errorf("failed setting default subvolume property to snapshot %d: %s", snapshot.ID, string(cmdOut))
		return err
	}

	err = b.updateBtrfsContext(b.stateDir)
	if err != nil {
		b.cfg.Logger.Errorf("got error updating snapshotter context %v", err)
		return err
	}

	// snapper has a special algorithm to clean snapshots.
	// do not rely on it, use an implementation similar to loop device
	// to handle pure btrfs snapshotter using same code
	err = b.cleanOldSnapshots()
	if err != nil {
		b.cfg.Logger.Warnf("got error cleaning old snapshots %v", err)
	}

	_ = b.setBootloader()

	return nil
}

func (b *Btrfs) DeleteSnapshot(id int) error {
	b.cfg.Logger.Infof("Deleting snapshot %d", id)
	var cmdOut []byte

	if id <= 0 {
		// ignore invalid ids
		return nil
	}

	if b.activeSnapshotID == id {
		err := fmt.Errorf("active snapshot can't be deleted")
		return err
	}

	if !b.btrfsCfg.DisableSnapper {
		if (b.activeSnapshotID == 0) && (id == 1) {
			// handle special bootstrap volume
			ok, err := b.deleteBootstrapSubvolume()

			if ok {
				return err
			} else {
				b.cfg.Logger.Debugf("snapshot %d not found, nothing has been deleted", id)
				return nil
			}
		}

		snapshots, err := b.GetSnapshots()
		if err != nil {
			b.cfg.Logger.Errorf("failed listing available snapshots: %v", err)
			return err
		}
		if !slices.Contains(snapshots, id) {
			b.cfg.Logger.Debugf("snapshot %d not found, nothing has been deleted", id)
			return nil
		}

		args := []string{"delete", "--sync", strconv.Itoa(id)}
		cmdOut, err := b.runCurrentSnapper(args...)
		if err != nil {
			b.cfg.Logger.Errorf("snapper failed deleting snapshot %d: %s", id, string(cmdOut))
			return err
		}
	} else {
		// Remove btrfs subvolume first
		snapshotDir := filepath.Join(filepath.Dir(b.snapshotsDir), fmt.Sprintf(snapshotPathTmpl, id))
		if ok, err := utils.Exists(b.cfg.Fs, snapshotDir); ok {
			args := []string{"subvolume", "delete", "-c", snapshotDir}
			cmdOut, err = b.cfg.Runner.Run("btrfs", args...)
			if err != nil {
				b.cfg.Logger.Errorf("failed deleting snapshot subvolume %d: %s", id, string(cmdOut))
				return err
			}
		} else if err != nil {
			b.cfg.Logger.Errorf("unable to stat snapshot subvolume %d: %s", id, snapshotDir)
			return err
		} else {
			b.cfg.Logger.Warnf("no snapshot subvolume %d exists", id)
		}

		// then remove associated directory
		parent := filepath.Dir(snapshotDir)
		if ok, err := utils.Exists(b.cfg.Fs, parent); ok {
			err := b.cfg.Fs.RemoveAll(parent)
			if err != nil {
				b.cfg.Logger.Errorf("failed deleting snapshot directory '%s': %s", parent, err)
				return err
			}
		} else if err != nil {
			b.cfg.Logger.Errorf("unable to stat snapshot directory %d: %s", id, snapshotDir)
			return err
		}
	}

	return nil
}

func (b *Btrfs) GetSnapshots() (snapshots []int, err error) {
	ids := []int{}

	if b.stateDir == "" {
		return ids, fmt.Errorf("unable to find btrfs state directory")
	}
	if !b.btrfsCfg.DisableSnapper {
		err = b.loadSnapperSnapshots(&ids)
		if err != nil {
			return ids, err
		}
	} else {
		list, err := b.cfg.Fs.ReadDir(b.snapshotsDir)
		if err != nil {
			b.cfg.Logger.Errorf("failed listing btrfs snapshots directory: %v", err)
			return ids, err
		}

		re := regexp.MustCompile(`^\d+$`)
		for _, entry := range list {
			if entry.IsDir() {
				entryName := entry.Name()

				if re.MatchString(entryName) {
					id, _ := strconv.Atoi(entryName)
					snapshotDir := filepath.Join(filepath.Dir(b.snapshotsDir), fmt.Sprintf(snapshotPathTmpl, id))

					if validateDirectory(b.cfg.Fs, snapshotDir) != "" {
						ids = append(ids, id)
					}
				}
			}
		}
	}

	return ids, err
}

func (b *Btrfs) loadSnapperSnapshots(ids *[]int) error {
	re := regexp.MustCompile(`^(\d+),(yes|no),(yes|no)$`)

	args := []string{"--csvout", "list", "--columns", "number,default,active"}
	cmdOut, err := b.runCurrentSnapper(args...)
	if err != nil {
		// snapper tries to relabel even when listing subvolumes, skip this error.
		if !strings.HasPrefix(string(cmdOut), "fsetfilecon on") {
			b.cfg.Logger.Errorf("failed collecting current snapshots: %s", string(cmdOut))
			return err
		}
	}

	scanner := bufio.NewScanner(strings.NewReader(strings.TrimSpace(string(cmdOut))))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		match := re.FindStringSubmatch(line)
		if match != nil {
			id, _ := strconv.Atoi(match[1])
			if id == 0 {
				continue
			}
			*ids = append(*ids, id)
		}
	}

	return nil
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

func (b *Btrfs) getSubvolumes(rootDir string) (btrfsSubvolList, error) {
	out, err := b.cfg.Runner.Run("btrfs", "subvolume", "list", "-a", "--sort=path", rootDir)
	if err != nil {
		b.cfg.Logger.Errorf("failed listing btrfs subvolumes: %v", err)
		return nil, err
	}
	return b.parseVolumes(strings.TrimSpace(string(out))), nil
}

func (b *Btrfs) getStateSubvolumes(rootDir string) (rootVolume *btrfsSubvol, snapshotsVolume *btrfsSubvol, err error) {
	volumes, err := b.getSubvolumes(rootDir)
	if err != nil {
		return nil, nil, err
	}

	snapshots := filepath.Join(rootSubvol, snapshotsPath)
	b.cfg.Logger.Debugf(
		"Looking for subvolumes %s and %s in subvolume list: %v",
		rootSubvol, snapshots, volumes,
	)
	for _, vol := range volumes {
		if vol.path == rootSubvol {
			rootVolume = &vol
		} else if vol.path == snapshots {
			snapshotsVolume = &vol
		}
	}

	return rootVolume, snapshotsVolume, err
}

func (b *Btrfs) getActiveSnapshot() (int, error) {
	re := regexp.MustCompile(snapshotPathRegex)
	if !b.btrfsCfg.DisableDefaultSubVolume {
		out, err := b.cfg.Runner.Run("btrfs", "subvolume", "get-default", b.stateDir)
		if err != nil {
			b.cfg.Logger.Errorf("failed getting default btrfs subvolume: %v", err)
			return 0, err
		}
		list := b.parseVolumes(strings.TrimSpace(string(out)))
		for _, v := range list {
			match := re.FindStringSubmatch(v.path)
			if match != nil {
				id, _ := strconv.Atoi(match[1])
				return id, nil
			}
		}
	} else {
		activeSnap := filepath.Join(b.stateDir, constants.ActiveSnapshot)
		activePath, err := b.cfg.Fs.Readlink(activeSnap)
		if err != nil {
			if os.IsNotExist(err) {
				return 0, nil
			}
			b.cfg.Logger.Errorf("failed reading active symlink %s: %v", activeSnap, err)
			return 0, err
		}
		b.cfg.Logger.Debugf("active snapshot path is %s", activePath)

		match := re.FindStringSubmatch(activePath)
		if match != nil {
			id, _ := strconv.Atoi(match[1])
			return id, nil
		}
	}
	return 0, nil
}

func (b *Btrfs) parseVolumes(rawBtrfsList string) btrfsSubvolList {
	re := regexp.MustCompile(`^ID (\d+) gen \d+ top level \d+ path (.*)$`)
	list := btrfsSubvolList{}

	scanner := bufio.NewScanner(strings.NewReader(rawBtrfsList))
	for scanner.Scan() {
		match := re.FindStringSubmatch(strings.TrimSpace(scanner.Text()))
		if match != nil {
			id, _ := strconv.Atoi(match[1])
			path := strings.TrimPrefix(match[2], "<FS_TREE>/")
			list = append(list, btrfsSubvol{id: id, path: path})
		}
	}
	return list
}

func (b *Btrfs) isInitiated(rootDir string) (bool, error) {
	rootVolume, snapshotsVolume, err := b.getStateSubvolumes(rootDir)
	if err != nil {
		return false, err
	}

	if (rootVolume != nil) && (snapshotsVolume != nil) {
		return true, nil
	}

	return false, nil
}

func (b *Btrfs) findSubvolumeByPath(path string) (int, error) {
	out, err := b.cfg.Runner.Run("btrfs", "inspect-internal", "rootid", path)
	if err != nil {
		b.cfg.Logger.Errorf("failed inspecting btrfs subvolume path: %v", err)
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(out)))
}

func (b *Btrfs) getPassiveSnapshots() ([]int, error) {
	passives := []int{}

	snapshots, err := b.GetSnapshots()
	if err != nil {
		return nil, err
	}
	for _, snapshot := range snapshots {
		if snapshot != b.activeSnapshotID {
			passives = append(passives, snapshot)
		}
	}

	return passives, nil
}

// cleanOldSnapshots deletes old snapshots to prevent exceeding the configured maximum
func (b *Btrfs) cleanOldSnapshots() error {
	var errs error

	b.cfg.Logger.Infof("Cleaning old passive snapshots")
	ids, err := b.getPassiveSnapshots()
	if err != nil {
		b.cfg.Logger.Warnf("could not get current snapshots")
		return err
	}

	sort.Ints(ids)
	for len(ids) > b.snapshotterCfg.MaxSnaps-1 {
		err = b.DeleteSnapshot(ids[0])
		if err != nil {
			b.cfg.Logger.Warnf("could not delete snapshot %d", ids[0])
			errs = multierror.Append(errs, err)
		}
		ids = ids[1:]
	}
	return errs
}

// setBootloader sets the bootloader variables to update new passives
func (b *Btrfs) setBootloader() error {
	var passives, fallbacks []string

	b.cfg.Logger.Infof("Setting bootloader with current passive snapshots")

	ids, err := b.getPassiveSnapshots()
	if err != nil {
		b.cfg.Logger.Warnf("failed getting passive snapshots: %v", err)
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
		constants.GrubActiveSnapshot:   strconv.Itoa(b.activeSnapshotID),
		"snapshotter":                  constants.BtrfsSnapshotterType,
	}

	err = b.bootloader.SetPersistentVariables(envFile, envs)
	if err != nil {
		b.cfg.Logger.Warnf("failed setting bootloader environment file %s: %v", envFile, err)
		return err
	}

	return err
}

func (b *Btrfs) configureSnapshotSnapper(snapshot *types.Snapshot) error {
	var extraPaths map[string]string

	rootconfig := filepath.Join(snapshot.WorkDir, snapperRootConfig)
	snapshotDir := filepath.Join(snapshot.WorkDir, snapshotsPath)

	if ok, err := utils.Exists(b.cfg.Fs, rootconfig); !ok {
		// ensure snapshots directory does not exists otherwise create config will fail
		err := utils.RemoveAll(b.cfg.Fs, snapshotDir)
		if err != nil {
			b.cfg.Logger.Errorf("unable to delete snapshots folder: %v", err)
			return err
		}

		// actually create the 'root' config file
		cmdOut, err := b.runSnapshotSnapper(snapshot.WorkDir, extraPaths,
			"--config", "root", "create-config",
			"--fstype", "btrfs", "/")
		if err != nil {
			b.cfg.Logger.Errorf("unable to create snapper root config: %v", err)
			b.cfg.Logger.Debugf("snapper output: %s", string(cmdOut))
			return err
		}

		// create config will create a '.snapshot' subvolume. delete it.
		args := []string{"subvolume", "delete", "-c", snapshotDir}
		cmdOut, err = b.cfg.Runner.Run("btrfs", args...)
		if err != nil {
			b.cfg.Logger.Errorf("failed deleting snapper .snapshot subvolume: %s", string(cmdOut))
			return err
		}
	} else if err != nil {
		b.cfg.Logger.Errorf("unable to stat snapper root config: %s", rootconfig)
		return err
	}

	// Make sure snapshots mountpoint folder is part of the resulting snapshot image
	err := utils.MkdirAll(b.cfg.Fs, snapshotDir, constants.DirPerm)
	if err != nil {
		b.cfg.Logger.Errorf("failed creating snapshots folder: %v", err)
		return err
	}

	// set global snapper configuration
	sysconfigData := map[string]string{}
	sysconfig := filepath.Join(snapshot.WorkDir, snapperDefaultconfig)
	if ok, _ := utils.Exists(b.cfg.Fs, sysconfig); !ok {
		sysconfig = filepath.Join(snapshot.WorkDir, snapperSysconfig)
	}

	if ok, _ := utils.Exists(b.cfg.Fs, sysconfig); ok {
		sysconfigData, err = utils.LoadEnvFile(b.cfg.Fs, sysconfig)
		if err != nil {
			b.cfg.Logger.Errorf("failed to load global snapper sysconfig")
			return err
		}
	}
	sysconfigData["SNAPPER_CONFIGS"] = "root"

	b.cfg.Logger.Debugf("Creating sysconfig snapper configuration at '%s'", sysconfig)
	err = utils.WriteEnvFile(b.cfg.Fs, sysconfigData, sysconfig)
	if err != nil {
		b.cfg.Logger.Errorf("failed writing snapper global configuration file: %v", err)
		return err
	}

	rootconfigData, err := utils.LoadEnvFile(b.cfg.Fs, rootconfig)
	if err != nil {
		b.cfg.Logger.Errorf("failed to load default snapper templage configuration")
		return err
	}

	rootconfigData["TIMELINE_CREATE"] = "no"
	rootconfigData["QGROUP"] = "1/0"
	rootconfigData["NUMBER_LIMIT"] = strconv.Itoa(b.snapshotterCfg.MaxSnaps)
	rootconfigData["NUMBER_LIMIT_IMPORTANT"] = strconv.Itoa(b.snapshotterCfg.MaxSnaps)

	b.cfg.Logger.Debugf("updating 'root' snapper configuration at '%s'", rootconfig)
	err = utils.WriteEnvFile(b.cfg.Fs, rootconfigData, rootconfig)
	if err != nil {
		b.cfg.Logger.Errorf("failed writing snapper root configuration file: %v", err)
		return err
	}

	return nil
}

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

	return b.updateBtrfsContext(state.Path)
}

func (b *Btrfs) setBtrfsForFirstTime(state *types.Partition) error {
	topDir, _, _, _, err := b.findStateMount(state.Path)
	if err != nil {
		b.cfg.Logger.Errorf("could not find expected btrfs top level directory")
		return err
	} else if topDir == "" {
		b.cfg.Logger.Errorf("btrfs root is not mounted, can't initialize the snapshotter within an existing subvolume")
		return err
	}

	b.cfg.Logger.Debug("Enabling btrfs quota")
	cmdOut, err := b.cfg.Runner.Run("btrfs", "quota", "enable", topDir)
	if err != nil {
		b.cfg.Logger.Errorf("failed setting quota for btrfs partition at %s: %s", topDir, string(cmdOut))
		return err
	}

	b.cfg.Logger.Debug("Creating essential subvolumes")
	for _, subvolume := range []string{filepath.Join(topDir, rootSubvol), filepath.Join(topDir, rootSubvol, snapshotsPath)} {
		b.cfg.Logger.Debugf("Creating subvolume: %s", subvolume)
		cmdOut, err = b.cfg.Runner.Run("btrfs", "subvolume", "create", subvolume)
		if err != nil {
			b.cfg.Logger.Errorf("failed creating subvolume %s: %s", subvolume, string(cmdOut))
			return err
		}
	}

	b.cfg.Logger.Debug("Create btrfs quota group")
	cmdOut, err = b.cfg.Runner.Run("btrfs", "qgroup", "create", "1/0", topDir)
	if err != nil {
		b.cfg.Logger.Errorf("failed creating quota group for %s: %s", topDir, string(cmdOut))
		return err
	}
	return nil
}

func (b *Btrfs) configureMountPointAndRootDir(state *types.Partition) error {
	err := b.updateBtrfsContext(state.Path)
	if b.stateDir == "" || b.rootDir == "" {
		err = fmt.Errorf("could not find expected mountpoints")
		return err
	}

	if err != nil {
		b.cfg.Logger.Errorf("failed setting snapper state partition mountpoint: %v", err)
		return err
	}

	// state.MountPoint must be updated otherwise state.yaml will fail to update
	state.MountPoint = b.stateDir

	return err
}

// simple convenience function which returns a given path only if it exists and is a directory
func validateDirectory(fs types.FS, path string) (validated string) {
	info, _ := fs.Lstat(path)
	if info != nil && info.IsDir() {
		validated = path
	}

	return validated
}

// General purpose function to retrieve all btrfs mount points for a given state partition
// incoming path can be either a disk device or the path of a mounted btrfs filesystem
// goal of this function is to be able to resolve path to all relevant btrfs directories
func (b *Btrfs) findStateMount(path string) (topDir string, rootDir string, snapshotsDir string, stateDir string, err error) {
	output, err := b.cfg.Runner.Run("findmnt", "-lno", "SOURCE,TARGET,FSTYPE", path)
	if err != nil {
		return "", "", "", "", err
	}

	// first pass accumulate findmnt lines.
	// This allow to ensure there is only one result when using a mounted btrfs filesystem path as argument
	var lines [][]string
	scanner := bufio.NewScanner(strings.NewReader(strings.TrimSpace(string(output))))
	for scanner.Scan() {
		lineFields := strings.Fields(scanner.Text())
		if len(lineFields) != 3 {
			continue
		}
		// Only handle lines with "btrfs" type
		if lineFields[2] == "btrfs" {
			lines = append(lines, lineFields)
		}
	}

	r := regexp.MustCompile(`^@/\.snapshots/\d+/snapshot$`)
	snapshotsSubvol := filepath.Join(rootSubvol, snapshotsPath)

	// second pass over parsed findmnt lines. search each mounted subvolume of interest.
	var rootDirMatches []string
	for _, lineFields := range lines {
		subStart := strings.Index(lineFields[0], "[/")

		// Additional recursive logic if array length is 1 and device matches target
		if len(lines) == 1 && path == lineFields[1] {
			// Handle subStart logic for recursive call
			if subStart != -1 {
				return b.findStateMount(lineFields[0][0:subStart])
			}
			return b.findStateMount(lineFields[0])
		}

		subEnd := strings.LastIndex(lineFields[0], "]")

		// Check if no subvolume is present
		if subStart == -1 && subEnd == -1 {
			topDir = lineFields[1] // this is the btrfs root
		} else {
			subVolume := lineFields[0][subStart+2 : subEnd]

			if subVolume == rootSubvol {
				stateDir = lineFields[1]
			} else if subVolume == snapshotsSubvol {
				snapshotsDir = lineFields[1]
			} else if r.MatchString(subVolume) && lineFields[1] == "/" {
				// only define rootDir if mounted as '/'
				rootDirMatches = append(rootDirMatches, lineFields[1])
			}
		}
	}

	// assume that is there is only one match for a snapshot, this is the rootDir
	if len(rootDirMatches) == 1 {
		rootDir = rootDirMatches[0]
	}

	// If stateDir isn't found but topDir exists, append the rootSubvol to topDir
	if stateDir == "" && topDir != "" {
		stateDir = validateDirectory(b.cfg.Fs, filepath.Join(topDir, rootSubvol))
	}

	// If snapshotsDir isn't found but rootDir or stateDir exists, append the subvolume to stateDir
	if snapshotsDir == "" && rootDir != "" {
		snapshotsDir = validateDirectory(b.cfg.Fs, filepath.Join(rootDir, snapshotsPath))
	}
	if snapshotsDir == "" && stateDir != "" {
		snapshotsDir = validateDirectory(b.cfg.Fs, filepath.Join(stateDir, snapshotsPath))
	}

	return topDir, rootDir, snapshotsDir, stateDir, err
}

func (b *Btrfs) updateBtrfsContext(path string) (err error) {
	_, rootDir, snapshotsDir, stateDir, err := b.findStateMount(path)
	if err == nil {
		b.rootDir = rootDir
		b.snapshotsDir = snapshotsDir
		b.stateDir = stateDir
		b.activeSnapshotID, err = b.getActiveSnapshot()
	}

	return err
}

func (b *Btrfs) deleteBootstrapSubvolume() (bool, error) {
	bootstrapDir := filepath.Join(b.stateDir, snapperBootstrapPath)

	if ok, err := utils.Exists(b.cfg.Fs, bootstrapDir); ok {
		args := []string{"subvolume", "delete", "-c", bootstrapDir}
		cmdOut, err := b.cfg.Runner.Run("btrfs", args...)
		if err != nil {
			b.cfg.Logger.Errorf("failed deleting bootstrap subvolume: %s", string(cmdOut))
			return false, err
		}

		return true, nil
	} else if err != nil {
		b.cfg.Logger.Errorf("unable to stat bootstrap subvolume: %s", bootstrapDir)
		return false, err
	}

	return false, nil
}

// wrapper function to execute snapper in the current context
func (b *Btrfs) runCurrentSnapper(args ...string) (out []byte, err error) {
	snapperArgs := []string{}

	if !(elemental.IsActiveMode(b.cfg) || elemental.IsPassiveMode(b.cfg)) {
		// Check if snapshots subvolume is mounted
		rootDir := filepath.Join(filepath.Dir(b.snapshotsDir), fmt.Sprintf(snapshotPathTmpl, b.activeSnapshotID))
		snapshotsSubvolume := filepath.Join(rootDir, snapshotsPath)
		if notMnt, _ := b.cfg.Mounter.IsLikelyNotMountPoint(snapshotsSubvolume); notMnt {
			err = b.cfg.Mounter.Mount(b.snapshotsDir, snapshotsSubvolume, "bind", []string{"bind"})
			if err != nil {
				return nil, err
			}
			defer func() {
				err := b.cfg.Mounter.Unmount(snapshotsSubvolume)

				if err != nil {
					b.cfg.Logger.Errorf("unable to find unmount snapper snapshot directory: %v", err)
				}
			}()
		}

		snapperArgs = []string{"--no-dbus", "--root", rootDir}
	}
	args = append(snapperArgs, args...)
	return b.cfg.Runner.Run("snapper", args...)
}

// wrapper function to execute snapper in the snapshot context
func (b *Btrfs) runSnapshotSnapper(rootDir string, extraPaths map[string]string, args ...string) (out []byte, err error) {
	callback := func() error {
		snapperArgs := []string{"--no-dbus"}
		args = append(snapperArgs, args...)
		out, err = b.cfg.Runner.Run("snapper", args...)
		return err
	}

	err = utils.ChrootedCallback(&b.cfg, rootDir, extraPaths, callback)
	return out, err
}
