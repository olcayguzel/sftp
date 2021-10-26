from actiontypes import ActionTypes


class FailAction:
    def __init__(self):
        self.Type: ActionTypes = ActionTypes.SkipFile
        self.Command = ""
        self.Args = ""
