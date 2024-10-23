import wx
import pylab as pl
import matplotlib
import numpy as np
from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas

class ScalarStatistic(wx.Panel):
    def __init__(self, parent, frame, elem_path):
        super(ScalarStatistic, self).__init__(parent)
        self.frame = frame
        self.elem_path = elem_path

        sizer = wx.BoxSizer(wx.HORIZONTAL)
        stat_values = frame.data_retriever.Unpack(elem_path)

        # Create a timeseries plot
        if len(stat_values['TimeVals']) > 0:
            self.time_vals = stat_values['TimeVals']
            self.data_vals = stat_values['DataVals']
            self.figure  = matplotlib.figure.Figure()
            self.canvas = FigureCanvas(self, -1, self.figure)
            self.canvas.SetPosition((25,-10))

            self.ax = self.figure.add_subplot(111)
            self.ax.plot(self.time_vals, self.data_vals, 'b-')
            self.ax.set_title(self.elem_path)
            self.ax.grid()
            self.ax.autoscale()

            # Add a ear button (size 16x16) to the left of the time series plot.
            # Clicking the button will open a dialog to change the plot settings.
            # Note that we do not add the button to the sizer since we want to
            # force it to be in the top-left corner of the widget canvas. We do
            # this with the 'pos' argument to the wx.BitmapButton constructor.
            gear_btn = wx.BitmapButton(self, bitmap=frame.CreateResourceBitmap('gear.png'), pos=(5,5))
            gear_btn.Bind(wx.EVT_BUTTON, self.__EditWidget)
            gear_btn.SetToolTip('Edit widget settings')
        else:
            sizer.Add(wx.StaticText(self, label='No data for stat at location:\n%s' % elem_path), 0, wx.EXPAND)

        self.SetSizer(sizer)
        self.Layout()

    def GetWidgetCreationString(self):
        return 'ScalarStatistic$' + self.elem_path

    def UpdateWidgetData(self):
        # Nothing to do since we plot all data
        pass

    def __EditWidget(self, event):
        # TODO
        print('Edit widget settings for %s' % self.elem_path)
