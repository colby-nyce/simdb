import copy, re

class SimHierarchy:
    def __init__(self, db):
        child_ids_by_parent_id = {}
        cursor = db.cursor()
        self._root_id = None
        self.__RecurseBuildHierarchy(cursor, 0, child_ids_by_parent_id)
        assert self._root_id is not None

        parent_ids_by_child_id = {}
        for parent_id, child_ids in child_ids_by_parent_id.items():
            for child_id in child_ids:
                parent_ids_by_child_id[child_id] = parent_id

        elem_names_by_id = {}
        cursor.execute('SELECT Id,Name FROM ElementTreeNodes')
        for id, name in cursor.fetchall():
            elem_names_by_id[id] = name

        dtype_by_elem_id = {}
        self._widget_types_by_elem_id = {}
        cursor.execute('SELECT ElementTreeNodeID,DataType FROM CollectableTreeNodes')
        for id, dtype in cursor.fetchall():
            dtype_by_elem_id[id] = dtype
            if '_contig_capacity' in dtype or '_sparse_capacity' in dtype:
                self._widget_types_by_elem_id[id] = 'QueueTable'
            elif dtype in ('char', 'int8_t', 'int16_t', 'int32_t', 'int64_t', 'uint8_t', 'uint16_t', 'uint32_t', 'uint64_t', 'float', 'double', 'bool'):
                self._widget_types_by_elem_id[id] = 'Timeseries'
            else:
                self._widget_types_by_elem_id[id] = 'ScalarStruct'

        def GetLineage(elem_id, parent_ids_by_child_id):
            lineage = []
            while elem_id not in (None,0,self._root_id):
                lineage.append(elem_id)
                elem_id = parent_ids_by_child_id.get(elem_id)

            lineage.reverse()
            return lineage
        
        def GetPath(elem_id, parent_ids_by_child_id, elem_names_by_id):
            lineage = GetLineage(elem_id, parent_ids_by_child_id)
            path = '.'.join([elem_names_by_id.get(elem_id) for elem_id in lineage])
            return path

        elem_paths_by_id = {}
        for elem_id in elem_names_by_id.keys():
            path = GetPath(elem_id, parent_ids_by_child_id, elem_names_by_id)
            if path:
                elem_paths_by_id[elem_id] = path

        self._scalar_stats_elem_paths = []
        self._scalar_structs_elem_paths = []
        self._container_elem_paths = []

        for id, dtype in dtype_by_elem_id.items():
            if dtype in ('int8_t', 'int16_t', 'int32_t', 'int64_t', 'uint8_t', 'uint16_t', 'uint32_t', 'uint64_t', 'float', 'double', 'bool'):
                self._scalar_stats_elem_paths.append(elem_paths_by_id[id])
            elif '_contig_capacity' in dtype or '_sparse_capacity' in dtype:
                self._container_elem_paths.append(elem_paths_by_id[id])
            else:
                self._scalar_structs_elem_paths.append(elem_paths_by_id[id])

        self._capacities_by_collectable_id = {}
        self._sparse_collectable_ids = set()
        self._contig_collectable_ids = set()
        for id, dtype in dtype_by_elem_id.items():
            if '_contig_capacity' in dtype:
                self._contig_collectable_ids.add(id)
            elif '_sparse_capacity' in dtype:
                self._sparse_collectable_ids.add(id)

            if '_contig_capacity' in dtype or '_sparse_capacity' in dtype:
                self._capacities_by_collectable_id[id] = int(dtype.split('_capacity')[1])

        self._child_ids_by_parent_id = child_ids_by_parent_id
        self._parent_ids_by_child_id = parent_ids_by_child_id
        self._elem_names_by_id = elem_names_by_id
        self._elem_paths_by_id = elem_paths_by_id
        self._elem_ids_by_path = {v: k for k, v in elem_paths_by_id.items()}

        collectable_ids_by_elem_path = {}
        for id, path in elem_paths_by_id.items():
            if id in self._widget_types_by_elem_id:
                collectable_ids_by_elem_path[path] = id

        elem_paths_by_collectable_id = {v:k for k,v in collectable_ids_by_elem_path.items()}
        self._collectable_id_by_elem_path = collectable_ids_by_elem_path

        # Sanity checks to ensure that no element path contains 'root.'
        for _,elem_path in self._elem_paths_by_id.items():
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._elem_ids_by_path.items():
            assert elem_path.find('root.') == -1

        for elem_path,_ in collectable_ids_by_elem_path.items():
            assert elem_path.find('root.') == -1

        for _,elem_paths in elem_paths_by_collectable_id.items():
            for elem_path in elem_paths:
                assert elem_path.find('root.') == -1

        for elem_path in self._scalar_stats_elem_paths:
            assert elem_path.find('root.') == -1

        for elem_path in self._scalar_structs_elem_paths:
            assert elem_path.find('root.') == -1

        for elem_path in self._container_elem_paths:
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._collectable_id_by_elem_path.items():
            assert elem_path.find('root.') == -1

    def GetRootID(self):
        return self._root_id

    def GetParentID(self, elem_id):
        return self._parent_ids_by_child_id[elem_id]

    def GetChildIDs(self, elem_id):
        return self._child_ids_by_parent_id.get(elem_id, [])
    
    def GetElemPath(self, elem_id):
        return self._elem_paths_by_id[elem_id]
    
    def GetElemID(self, elem_path):
        return self._elem_ids_by_path.get(elem_path)
    
    def GetCollectionID(self, elem_path):
        return self._collectable_id_by_elem_path.get(elem_path)

    def GetContainerIDs(self):
        return self._contig_collectable_ids | self._sparse_collectable_ids

    def GetCapacityByCollectionID(self, collectable_id):
        return self._capacities_by_collectable_id.get(collectable_id)
    
    def GetCapacityByElemPath(self, elem_path):
        collectable_id = self.GetCollectionID(elem_path)
        return self.GetCapacityByCollectionID(collectable_id)
    
    def GetSparseFlagByCollectionID(self, collectable_id):
        return collectable_id in self._sparse_collectable_ids

    def GetSparseFlagByElemPath(self, elem_path):
        collectable_id = self.GetCollectionID(elem_path)
        return self.GetSparseFlagByCollectionID(collectable_id)

    def GetName(self, elem_id):
        return self._elem_names_by_id[elem_id]
    
    def GetElemPaths(self):
        return self._elem_paths_by_id.values()

    def GetScalarStatsElemPaths(self):
        return copy.deepcopy(self._scalar_stats_elem_paths)
    
    def GetScalarStructsElemPaths(self):
        return copy.deepcopy(self._scalar_structs_elem_paths)
    
    def GetContainerElemPaths(self):
        return copy.deepcopy(self._container_elem_paths)

    def GetItemElemPaths(self):
        elem_paths = self.GetScalarStatsElemPaths() + self.GetScalarStructsElemPaths() + self.GetContainerElemPaths()
        elem_paths.sort()
        return elem_paths
    
    def GetWidgetType(self, elem_id):
        return self._widget_types_by_elem_id.get(elem_id, '')

    def GetElemPathsMatchingRegex(self, elem_path_regex):
        return [elem_path for elem_path in self.GetElemPaths() if re.match(elem_path_regex)]

    def __RecurseBuildHierarchy(self, cursor, parent_id, child_ids_by_parent_id):
        cursor.execute("SELECT Id FROM ElementTreeNodes WHERE ParentID={}".format(parent_id))
        child_rows = cursor.fetchall()

        for row in child_rows:
            if self._root_id is None and parent_id == 0:
                self._root_id = row[0]
            elif self._root_id is not None and parent_id == 0:
                raise Exception('Multiple roots found in hierarchy')

            child_id = row[0]
            child_ids_by_parent_id[parent_id] = child_ids_by_parent_id.get(parent_id, []) + [child_id]
            self.__RecurseBuildHierarchy(cursor, child_id, child_ids_by_parent_id)
