package surfstore

import (
	"errors"
	"fmt"
)

type MetaStore struct {
	FileMetaMap map[string]FileMetaData
}

func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	// panic("todo")
	for key, element := range m.FileMetaMap {
		(*serverFileInfoMap)[key] = element
	}
	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	// panic("todo")
	fmt.Println("start update server")
	fn := fileMetaData.Filename
	fmd, ok := m.FileMetaMap[fn]
	if ok {
		v_input := fileMetaData.Version
		v_hash := fmd.Version
		if v_input > v_hash {
			(m.FileMetaMap)[fn] = (*fileMetaData)
			*latestVersion = fileMetaData.Version
		} else if v_input < v_hash {
			return errors.New("trying to update an older version")
		} else { // ==

		}
	} else {
		m.FileMetaMap[fn] = (*fileMetaData)
		*latestVersion = 1 // should be the first
	}
	return nil
}

var _ MetaStoreInterface = new(MetaStore)
