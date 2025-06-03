import zlib, struct
from viewer.gui.widgets.scalar_statistic import ScalarStatistic

class IPCWidget(ScalarStatistic):
    def __init__(self, parent, frame):
        super().__init__(parent, frame, 'IPC', configurable=False, size=parent.GetSize())

        self.ax.set_title('')
        self.ax.set_xlabel('')
        self.ax.set_ylabel('IPC')
        self.ax.grid(False)
        self.ax.tick_params(axis='x', which='both', bottom=False, top=False)
        self.ax.set_xlim(left=self.time_vals[0], right=self.time_vals[-1])

        r,g,b = (240/255 for _ in range(3))
        self.figure.patch.set_facecolor((r,g,b))
        self._avxline = None

        self.UpdateWidgetData()

        # Launch frame inspector
        import wx.lib.inspection
        wx.lib.inspection.InspectionTool().Show()

    def GetWidgetCreationString(self):
        return 'IPC'

    def UpdateWidgetData(self):
        self.__UpdatePlotSize()
        self.__UpdateCurrentTickVertLine()
        self.canvas.draw()
        self.Layout()
        self.Update()
        self.Refresh()

    def GetCurrentViewSettings(self):
        return {}
    
    def GetCurrentUserSettings(self):
        return {}

    def ApplyViewSettings(self, settings):
        pass

    def GetStatsValues(self, frame, elem_path):
        cursor = frame.db.cursor()

        cmd = 'SELECT DataValsBlob, IsCompressed FROM TimeseriesData WHERE ElementPath="{}"'.format(elem_path)
        cursor.execute(cmd)

        data_vals_blob, is_compressed = cursor.fetchone()
        if is_compressed:
            data_vals_blob = zlib.decompress(data_vals_blob)

        data_vals = []
        while len(data_vals_blob) > 0:
            val_blob = data_vals_blob[:8]
            data_vals.append(struct.unpack('d', val_blob)[0])
            data_vals_blob = data_vals_blob[8:]

        return data_vals
    
    def __UpdatePlotSize(self):
        # Desired size in pixels
        desired_width_pixels = self.GetParent().GetSize().GetWidth() - 10
        desired_height_pixels = 50

        # Get the current DPI
        dpi = self.figure.get_dpi()

        # Calculate new size in inches
        new_width_inches = desired_width_pixels / dpi
        new_height_inches = desired_height_pixels / dpi

        # Set the new size in inches
        self.figure.set_size_inches(new_width_inches, new_height_inches)

    def __UpdateCurrentTickVertLine(self):
        if self._avxline:
            self._avxline.remove()

        tick = self.frame.widget_renderer.tick
        if tick in self.time_vals:
            self._avxline = self.ax.axvline(x=tick, color='r', linestyle='--')
