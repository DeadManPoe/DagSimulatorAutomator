import csv
import sys
import os

class Parser:
    def __init__(self, jobsFile,stagesFile,stagesRelfile):
        self.targetDirectory=''
        self.stagesJobMap = {}
        self.jobStageMap = {}
        self.stagesRows = []
        self.jobs = []
        self.jobsFile = jobsFile
        self.stagesRelFile = stagesRelfile
        self.stagesFile = stagesFile
        map(lambda x: self.fileValidation(x),[jobsFile,stagesFile, stagesRelfile])
        self.fileValidation(stagesFile)
        self.parseJobs()
        f = open(stagesFile, "r")
        self.stagesRows = self.orderStages(csv.DictReader(f))
        self.parseStages()
        self.buildOutputString()

    def fileValidation(self,filename):
        if not(os.path.exists(filename)):
            print("The file "+filename+" does not exists")
            exit(-1)

    def parseJobs(self):
        f = open(self.jobsFile,"r")
        jobsReader = csv.DictReader(f)
        for row in jobsReader:
            stageIds = row["Stage IDs"]
            jobId = row["Job ID"]
            if(stageIds!= "NOVAL"):
                self.parseStageList(jobId,stageIds)
                self.jobs.append({"job_id":jobId, "stages":stageIds[1:len(stageIds)-1].split(", ")})
        f.close()

    def orderStages(self,stages):
        return sorted(stages, key = lambda x: x["Stage ID"])

    def parseStages(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if((lastRow != None and lastRow["Stage ID"] != row["Stage ID"])):
                f = open("./tmp_output/J"+self.stagesJobMap[lastRow["Stage ID"]]+"S"+lastRow["Stage ID"]+".txt","w")
                f.write(", ".join(batch))
                f.close()
                batch = []
            batch.append(row["Executor Run Time"])
            lastRow = row

    def parseStageList(self, jobId, stageIds):
        stages = stageIds[1:len(stageIds)-1].split(", ")
        for stage in stages:
            self.stagesJobMap[stage]=jobId

    def reconstructRel(self):
        f = open(self.stagesRelFile,"r")
        rows = self.orderStages(csv.DictReader(f))
        stagesMap = {}
        for row in rows:
            parentIds = row["Parent IDs"]
            stageId = row["Stage ID"]
            parents = parentIds[1:len(parentIds)-1].split(", ")
            if(len(parents)== 1 and parents[0] == ''):
                parents = []
            stagesMap[stageId]= {
                "parents": parents,
                "children": [],
                "tasks": row["Number of Tasks"],
                "name": "S"+stageId
            }
            for parent in parents:
                stagesMap[parent]["children"].append(stageId)

        return stagesMap

    def perJobReconstructRel(self):
        stagesMap = self.reconstructRel()
        tmpFirst = []
        tmpLast = []
        newMap = []
        sortedMap = {}
        sortedJobs = sorted(self.jobs, key=lambda x: x["job_id"])
        maxJob = sortedJobs[len(sortedJobs)-1]["job_id"]
        for job in sortedJobs:
            for stage in job["stages"]:
                stagesMap[stage]["name"] = "J"+job["job_id"]+stagesMap[stage]["name"]
                if(len(stagesMap[stage]["children"])==0):
                    tmpLast.append(stage)
                if(len(stagesMap[stage]["parents"])==0):
                    tmpFirst.append(stage)
            newMap.append({
                "job_id" : job["job_id"],
                "stages" : job["stages"],
                "last": tmpLast,
                "first": tmpFirst
            })
            tmpLast = []
            tmpFirst = []

        for i,job in enumerate(newMap):
            if(job["job_id"] != maxJob):
                for stage in job["last"]:
                    for stage_1 in newMap[i+1]["first"]:
                        stagesMap[stage_1]["parents"].append(stage)
                        stagesMap[stage]["children"].append(stage_1)

        return(stagesMap)

    def buildOutputString(self):
        stagesMap = self.perJobReconstructRel()
        targetString = ''
        for key,value in stagesMap.iteritems():
            namedParents = map(lambda x: stagesMap[x]["name"], value["parents"])
            namedChildren = map(lambda x: stagesMap[x]["name"], value["children"])
            namedParents = reduce(lambda accumul, current: accumul+'"'+current+'",',namedParents, '' )
            namedChildren = reduce(lambda accumul, current: accumul+'"'+current+'",',namedChildren, '' )
            if(namedParents!=''):
                namedParents = namedParents[:len(namedParents)-1]
            if(namedChildren!=''):
                namedChildren = namedChildren[:len(namedChildren)-1]
            targetString+='{ name="'+value["name"]+'", tasks="'+value["tasks"]+'"'
            targetString+=', dist={type="replay", params={samples=solver.fileToArray("'+self.targetDirectory+value["name"]+'.txt")}}'
            targetString+=', pre={'+namedParents+'}, post={'+namedChildren+'}},'
        targetString = '{'+targetString[:len(targetString)-1]+'}'
        print(targetString)



def main():
    args = sys.argv
    if len(args) != 4:
        print("Required args: [JOBS_FILE_CSV] [STAGE_FILE_CSV] [STAGE_REL_FILE_CSV]")
        exit(-1)
    else:
        parser = Parser(str(args[1]),str(args[2]),str(args[3]))

if(__name__=="__main__"):
    main()
