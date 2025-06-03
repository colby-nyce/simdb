import wx
from viewer.gui.view_settings import DirtyReasons

class DirtySplitterWindow(wx.SplitterWindow):
    def __init__(self, frame, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.frame = frame
        self.Bind(wx.EVT_SPLITTER_SASH_POS_CHANGED, self.__OnSashPosChanged)

    def __OnSashPosChanged(self, event):
        self.frame.view_settings.SetDirty(reason=DirtyReasons.SashPositionChanged)
        event.Skip()
