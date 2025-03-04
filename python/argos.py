import wx, sys, argparse
from viewer.model.workspace import Workspace

class MyApp(wx.App):
    def OnInit(self):
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, help="Path to the database file")
    parser.add_argument("--view-file", help="Path to the view file (*.avf) to load")
    parser.add_argument("--dev-debug", help="Enable verbose output for development and debugging", action="store_true")
    args = parser.parse_args()

    app = MyApp()
    workspace = Workspace(args.database, args.view_file, args.dev_debug)
    app.MainLoop()
