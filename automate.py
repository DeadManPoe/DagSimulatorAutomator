import csv
import sys
import os

class Parser:
    def __init__(self, jobsFile,stagesFile,stagesRelfile,targetDirectory):
        self.targetDirectory= targetDirectory
        self.stageJobMap = {}
        self.jobStageMap = {}
        self.stagesRows = []
        self.jobs = []
        self.jobsTemporalMap = {}
        self.superMap = {}
        self.jobsFile = jobsFile
        self.stagesRelFile = stagesRelfile
        self.stagesFile = stagesFile
        map(lambda x: self.fileValidation(x),[jobsFile,stagesFile, stagesRelfile])
        self.parseJobs()
        self.buildJobsHierachy()
        #f = open(stagesFile, "r")
        #self.stagesRows = self.orderStages(csv.DictReader(f))
        #f.close()
        #self.buildTimeFiles()
        #self.buildOutputString()

    def fileValidation(self,filename):
        if not(os.path.exists(filename)):
            print("The file "+filename+" does not exists")
            exit(-1)

    def parseJobs(self):
        jobs = {}
        f = open(self.jobsFile,"r")
        jobsReader = csv.DictReader(f)
        for row in jobsReader:
            stageIds = row["Stage IDs"]
            jobId = row["Job ID"]
            completionTime = row["Completion Time"]
            submissionTime = row["Submission Time"]
            if(submissionTime != "NOVAL"):
                self.jobsTemporalMap[jobId] = {
                    "submissionTime" : int(submissionTime),
                    "completionTime" : 0
                }
            if(completionTime != "NOVAL"):
                self.jobsTemporalMap[jobId]["completionTime"] = int(completionTime)

            if(stageIds != "NOVAL"):
                stagesList = self.parseStagesList(stageIds)
                for stage in stagesList:
                    self.stageJobMap[stage]=jobId
                self.jobs.append({"job_id":jobId, "stages":self.parseStagesList(stageIds)})
        f.close()

    def orderStages(self,stages):
        return sorted(stages, key = lambda x: x["Stage ID"])

    def parseStagesList(self,stagesList):
        return stagesList[1:len(stagesList)-1].split(", ")

    def buildJobsHierachy(self):
        hierarchy = {}
        for key,value in self.jobsTemporalMap.iteritems():
            for key_1, value_1 in self.jobsTemporalMap.iteritems():
                if(value["completionTime"] < value_1["submissionTime"] and key != key_1):
                    try:
                        hierarchy[key]["followers"].append(key_1)
                    except KeyError:
                        hierarchy[key] = {
                            "followers" : [key_1],
                            "parents" : []
                        }
                    try:
                        hierarchy[key_1]["parents"].append(key)
                    except KeyError:
                        hierarchy[key_1] = {
                            "followers" : [],
                            "parents" : [key]
                        }
        jobFixingMap = {}
        for key,value in hierarchy.iteritems():
            print(key)
            target = []
            sort = sorted(value["parents"], key=lambda x: self.jobsTemporalMap[key]["submissionTime"] - self.jobsTemporalMap[x]["completionTime"])
            print(sort)
            if(len(sort) != 0):
                target.append(sort[0])
            self.recurrParent(sort,hierarchy,target)
            jobFixingMap[key] = {
                "parents" : target,
                "followers" : []
            }
        self.buildChildren(jobFixingMap)
        self.superMap = jobFixingMap



    def buildChildren(self,jobFixingMap):
        for key,value in jobFixingMap.iteritems():
            for key_1, value_1 in jobFixingMap.iteritems():
                if(key != key_1 and key in value_1["parents"]):
                    value["followers"].append(key_1)

    def recurrParent(self,sort,hierarchy,target):
        counter = 0
        for index, value in enumerate(sort):
            if(index != 0):
                for index_1, value_1 in enumerate(sort[:index]):
                    if(value not in hierarchy[sort[index_1]]["parents"]):
                        counter=counter+1
                if(counter == len(sort[:index])):
                    target.append(value)
                counter = 0


    def buildTimeFiles(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if((lastRow != None and lastRow["Stage ID"] != row["Stage ID"])):
                f = open("./output/J"+self.stageJobMap[lastRow["Stage ID"]]+"S"+lastRow["Stage ID"]+".txt","w")
                f.write(", ".join(batch))
                f.close()
                batch = []
            batch.append(row["Executor Run Time"])
            lastRow = row

    def stagesRel(self):
        f = open(self.stagesRelFile,"r")
        rows = self.orderStages(csv.DictReader(f))
        stagesMap = {}
        for row in rows:
            parentIds = row["Parent IDs"]
            stageId = row["Stage ID"]
            parents = self.parseStagesList(parentIds)
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

    def perJobStagesRel(self):
        stagesMap = self.stagesRel()
        tmpFirst = []
        tmpLast = []
        newMap = []
        sortedJobs = sorted(self.jobs, key=lambda x: x["job_id"])
        maxJob = sortedJobs[len(sortedJobs)-1]["job_id"]
        #For each job retrieve the first stages and the last stages
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
        stagesDict = self.perJobStagesRel()
        targetString = ''
        for key,value in stagesDict.iteritems():
            namedParents = map(lambda x: stagesDict[x]["name"], value["parents"])
            namedChildren = map(lambda x: stagesDict[x]["name"], value["children"])
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
    if len(args) != 5:
        print("Required args: [JOBS_FILE_CSV] [STAGE_FILE_CSV] [STAGE_REL_FILE_CSV] [DIRECTORY_FOR_OUTPUTTED_STRING]")
        exit(-1)
    else:
        parser = Parser(str(args[1]),str(args[2]),str(args[3]),str(args[4])+'/')

if(__name__=="__main__"):
    main()
