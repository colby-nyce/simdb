import wx
from viewer.gui.widgets.queue_utiliz import QueueUtilizWidget
from viewer.gui.widgets.packet_tracker import PacketTrackerWidget
from viewer.gui.widgets.scheduling_lines import SchedulingLinesWidget
from viewer.gui.widgets.timeseries_viewer import TimeseriesViewerWidget
from viewer.gui.widgets.scalar_statistic import ScalarStatistic
from viewer.gui.widgets.scalar_struct import ScalarStruct
from viewer.gui.widgets.iterable_struct import IterableStruct

class WidgetCreator:
    def __init__(self, frame):
        self.frame = frame

    def BindToWidgetSource(self, widget_source):
        if isinstance(widget_source, wx.TreeCtrl):
            widget_source.Unbind(wx.EVT_TREE_BEGIN_DRAG)
            widget_source.Bind(wx.EVT_TREE_BEGIN_DRAG, self.__BeginDragFromTree)

    def CreateWidget(self, widget_creation_key, widget_container):
        if widget_creation_key == 'Queue Utilization':
            return QueueUtilizWidget(widget_container, self.frame)
        elif widget_creation_key == 'Packet Tracker':
            return PacketTrackerWidget(widget_container, self.frame)
        elif widget_creation_key == 'Scheduling Lines':
            return SchedulingLinesWidget(widget_container, self.frame)
        elif widget_creation_key == 'Timeseries Viewer':
            return TimeseriesViewerWidget(widget_container, self.frame)
        elif widget_creation_key.find('$') != -1:
            widget_name, elem_path = widget_creation_key.split('$')
            if widget_name == 'ScalarStatistic':
                return ScalarStatistic(widget_container, self.frame, elem_path)
            elif widget_name == 'ScalarStruct':
                return ScalarStruct(widget_container, self.frame, elem_path)
            elif widget_name == 'IterableStruct':
                return IterableStruct(widget_container, self.frame, elem_path)
        elif widget_creation_key == 'NO_WIDGET':
            return None
        else:
            raise ValueError(f"Unknown widget creation key: {widget_creation_key}")

    def __BeginDragFromTree(self, event):
        tree = event.GetEventObject()
        item = event.GetItem()
        item_parent = tree.GetItemParent(item)
        if not item_parent.IsOk():
            event.Skip()
            return

        widget_creation_str = None
        if tree.GetItemText(item_parent) == 'Systemwide Tools':
            widget_creation_str = tree.GetItemText(item)
        else:
            elem_path = tree.GetItemElemPath(item)
            simhier = self.frame.explorer.navtree.simhier

            if elem_path in simhier.GetScalarStatsElemPaths():
                widget_creation_str = f'ScalarStatistic${elem_path}'
            elif elem_path in simhier.GetScalarStructsElemPaths():
                widget_creation_str = f'ScalarStruct${elem_path}'
            elif elem_path in simhier.GetContainerElemPaths():
                widget_creation_str = f'IterableStruct${elem_path}'

        if widget_creation_str is None:
            event.Skip()
            return

        data = wx.TextDataObject()
        data.SetText(widget_creation_str)
        source = wx.DropSource(tree)
        source.SetData(data)
        source.DoDragDrop(True)
