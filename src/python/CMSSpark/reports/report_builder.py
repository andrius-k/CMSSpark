class ReportBuilder():

    def __init__(self):
        self.report = ""

    def append(self, value):
        self.report += value
    
    def get(self):
        return self.report
    