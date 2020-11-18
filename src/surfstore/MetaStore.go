package surfstore

import (
	"errors"
)

type MetaStore struct {
	FileMetaMap map[string]FileMetaData
}

func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	for key, element := range m.FileMetaMap {
		(*serverFileInfoMap)[key] = element
	}
	return nil
}

func (m *MetaStore) UpdateFile(newFileMeta *FileMetaData, latestVersion *int) (err error) {
	filename := newFileMeta.Filename
	if fileMeta, ok := m.FileMetaMap[filename]; ok {
		if newFileMeta.Version > fileMeta.Version {
			(m.FileMetaMap)[filename] = (*newFileMeta)
			*latestVersion = newFileMeta.Version
		} else if newFileMeta.Version < fileMeta.Version {
			err = errors.New("trying to update an older version")
		}
	} else {
		m.FileMetaMap[filename] = (*newFileMeta)
		*latestVersion = newFileMeta.Version
	}

	return err
}

var _ MetaStoreInterface = new(MetaStore)
