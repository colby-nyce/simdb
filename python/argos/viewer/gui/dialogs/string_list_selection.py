import wx

class StringListSelectionDlg(wx.Dialog):
    def __init__(self, widget, all_strings, checked_strings, prompt='Make selections:'):
        super().__init__(widget, title='Customize Widget')

        self.checkboxes = []
        panel = wx.Panel(self)
        sizer = wx.BoxSizer(wx.VERTICAL)

        tbox = wx.StaticText(panel, label=prompt)
        sizer.Add(tbox, 0, wx.ALL | wx.EXPAND, 5)
 
        # Create a checkbox for each item
        for s in all_strings:
            checkbox = wx.CheckBox(panel, label=s)
            checkbox.Bind(wx.EVT_CHECKBOX, self.__OnCheckbox)
            sizer.Add(checkbox, 1, wx.ALL | wx.EXPAND, 5)

            if s in checked_strings:
                checkbox.SetValue(True)
            else:
                checkbox.SetValue(False)

            self.checkboxes.append(checkbox)

        # OK and Cancel buttons
        self._ok_btn = wx.Button(panel, wx.ID_OK)
        btn_sizer = wx.StdDialogButtonSizer()
        btn_sizer.AddButton(self._ok_btn)
        btn_sizer.AddButton(wx.Button(panel, wx.ID_CANCEL))
        btn_sizer.Realize()

        sizer.Add(btn_sizer, 0, wx.ALIGN_CENTER | wx.ALL, 10)
        panel.SetSizerAndFit(sizer)

        # Find the longest string length of all our checkbox labels
        dc = wx.ClientDC(panel)
        dc.SetFont(wx.Font(10, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL))
        longest_length = 0

        for s in all_strings:
            text_width, _ = dc.GetTextExtent(s)
            longest_length = max(longest_length, text_width)

        w,h = sizer.GetMinSize()
        h += 100
        w = max(w, longest_length + 100)
        self.SetSize((w,h))
        self.Layout()
        self.Refresh()

    def GetSelectedStrings(self):
        # Return a list of selected items
        return [checkbox.GetLabel() for checkbox in self.checkboxes if checkbox.IsChecked()]

    def __OnCheckbox(self, event):
        any_selected = any(checkbox.IsChecked() for checkbox in self.checkboxes)
        self._ok_btn.Enable(any_selected)
