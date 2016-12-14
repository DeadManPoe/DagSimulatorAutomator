import csv
import sys
import os

class Parser:
    def __init__(self, jobsFile,stagesFile):
        self.stagesJobMap = {}
        self.stagesRows = []
        self.jobsFile = jobsFile
        self.stagesFile = stagesFile
        self.fileValidation(jobsFile)
        self.fileValidation(stagesFile)
        self.parseJobs()
        self.orderStages()
        self.parseStages()

    def fileValidation(self,filename):
        if not(os.path.exists(filename)):
            print("The file "+filename+" does not exists")
            exit(-1)

    def parseJobs(self):
        f = open(self.jobsFile,"r")
        jobsReader = csv.DictReader(f)
        for row in jobsReader:
            stageIds = row["Stage IDs"]
            if(stageIds!= "NOVAL"):
                self.parseStageList(row["Job ID"],stageIds)
        f.close()

    def orderStages(self):
        f = open(self.stagesFile, "r")
        stagesReader = csv.DictReader(f)
        self.stagesRows = sorted(stagesReader, key = lambda x: x["Stage ID"])
        f.close()

    def parseStages(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if((lastRow != None and lastRow["Stage ID"] != row["Stage ID"])):
                f = open("./tmp_output/"+self.stagesJobMap[lastRow["Stage ID"]]+"_"+lastRow["Stage ID"]+".txt","w")
                f.write(", ".join(batch))
                f.close()
                print(len(batch))
                batch = []
            batch.append(row["Executor Run Time"])
            lastRow = row

    def parseStageList(self, jobId, stageIds):
        stages = stageIds[1:len(stageIds)-1].split(", ")
        for stage in stages:
            self.stagesJobMap[stage]=jobId
def main():
    args = sys.argv
    if len(args) != 3:
        print("Required args: [JOBS_FILE_CSV] [STAGE_FILE_CSV]")
        exit(-1)
    else:
        print
        parser = Parser(str(args[1]),str(args[2]))


if(__name__=="__main__"):
    main()
