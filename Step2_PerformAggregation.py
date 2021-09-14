#-------------------------------------------------------------------------------
# Name:        Neighboring parcel ownership aggregations
# Purpose:     Identify and group neighboring parcels with common ownership
#
# Author:      shannon.thol
#
# Created:     03/04/2020
# Copyright:   (c) shannon.thol 2020
# Licence:     <your licence>
#-------------------------------------------------------------------------------

#load necessary packages
import arcpy
from arcpy import env


import sys, re,csv
sys.path.append(r"C:\gisdata\resources\conda\arcgispro-py3-clone\Lib\site-packages")
from fuzzywuzzy import fuzz
import time

#create initial start time reference
initialStart = time.time()

arcpy.env.overwriteOutput = True

########################################################################################################################################################################################################################
#Specify criteria for finding neighboring parcels

#dist is the maximum distance (polygon edge to edge) to search away from the target parcel when identifying neighbors
dist = "50 Meters" #50 meters will capture immediately adjacent neighbors and those across a right-of-way

#count is the maximum number of features to return when identifying neighbors
count = 1000 #1000 features will capture all neighbors with an extremely generous buffer

#Specify criteria for aggregating owernships in fuzzy string matching
#ownThresh is the fuzzy string matching score threshold to use when deciding if name strings are similar enough to be classified as part of the same owernship collection
ownThresh = 93

#compLen is the length comparison threshold to use when deciding if two strings should be compared for determining if they are part of the same ownership collection
compLen = 1.25

#########################################################################################################################################################################################################################
#Get path to BigQuery results in csv file
##bigQuery = r"C:\gisdata\projects\NYParcelAggregation\BigQuery_ny_adjacency_results\NY_Manhattan_ParcelAdjacency.csv"

#Get list of paths to BigQuery results in csv files
bqResults = [r"C:\gisdata\projects\NYParcelAggregation\BigQuery_ny_adjacency_results\NY_Central_ParcelAdjacency.csv", r"C:\gisdata\projects\NYParcelAggregation\BigQuery_ny_adjacency_results\NY_Manhattan_ParcelAdjacency.csv",
r"C:\gisdata\projects\NYParcelAggregation\BigQuery_ny_adjacency_results\NY_NorthEast_ParcelAdjacency.csv", r"C:\gisdata\projects\NYParcelAggregation\BigQuery_ny_adjacency_results\NY_South_ParcelAdjacency.csv",
r"C:\gisdata\projects\NYParcelAggregation\BigQuery_ny_adjacency_results\NY_Southeast_ParcelAdjacency.csv", r"C:\gisdata\projects\NYParcelAggregation\BigQuery_ny_adjacency_results\NY_West_ParcelAdjacency.csv"]

#Get path to parcel dataset
parcels = r"C:\gisdata\projects\NYParcelAggregation\NY_Statewide.gdb\ny_statewide"

#Get path to statewide parcel dataset
state = r"C:\gisdata\projects\NYParcelAggregation\NY_Statewide.gdb\ny_statewide"

#Get path to scratch gdb where all intermediate results will be written
scratch = r"C:\gisdata\projects\NYParcelAggregation\scratch.gdb"

#Get name of ID field in the parcel dataset
idField = "PARCEL_ID"

#Get name of owner name field in the parcel dataset
ownField = "OWNER"

#Specify name of the collection id field in the parcel dataset
collField = "COLL_ID"

#########################################################################################################################################################################################################################
#read results from BigQuery table into dictionary of neighboring parcels
nghbrDict = dict() ##nghbrDict = {focPar: [nearPar1, nearPar2, nearPar3 ...] ...}
for bigQuery in bqResults:
    print ("loading " + bigQuery)
    with open(bigQuery) as csvFile:
        readCsv = csv.reader(csvFile, delimiter = ',')
        counter = 0
        for row in readCsv:
            if counter == 0:
                pass
            else:
                focal = int(row[0])
                nghbr = int(row[1])
                if focal in nghbrDict:
                    currVals = nghbrDict[focal]
                    currVals.append(nghbr)
                else:
                    nghbrDict[focal] = [nghbr]
            counter+=1

#Create empty dictionary for storing ID numbers, owner names, and owner addresses
ownDict = dict()  #ownDict = {PARC_ID: PARC_OWN ...}

#Iterate through features in the statewide parcel dataset, populating ownDict dictionary with parcel ID numbers and owner names
print ("Creating reference dictionary of parcel IDs and owner names ...")
with arcpy.da.SearchCursor(state, [idField, ownField]) as cursor:
    for row in cursor:
        parcId = int(row[0])
        if row[1] is None:
            parcOwn = ""
        elif row[1].replace(" ","") == "":
            parcOwn = ""
        else:
            parcOwn = re.sub(' +', ' ',row[1].replace(","," ").replace("."," ").replace(";"," ").replace("-"," ").replace(":"," ").strip())
        ownDict[parcId] = parcOwn

#Create empty dictionary for storing parcel collection information in the form #collPar = {coll ID = [parcel ID1, parcel ID2, etc.]}
collPar = dict()

#Create empty dictionary for storing parcel collection assignments in the form #parColl = {parcel ID: coll ID}
parColl = dict()

#Set up counter and collection ID
i = 1
collId = i

x = 1
xList = [1, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000, 1100000, 1200000, 1300000, 1400000, 1500000, 1600000, 1700000, 1800000, 1900000, 2000000, 2100000, 2200000, 2300000, 2400000, 2500000,
2600000, 2700000, 2800000, 2900000, 3000000, 3100000, 3200000, 3300000, 3400000, 3500000, 3600000, 3700000, 3800000, 3900000, 4000000, 4100000, 4200000, 4300000, 4400000, 4500000, 4600000, 4700000, 4800000, 4900000, 5000000,
5100000, 5200000, 5300000, 5400000, 5500000, 5600000, 5700000, 5800000, 5900000, 6000000]

start = time.time()
#Iterate through features in parcel dataset again, identifying near features and assessing similarity of their owner names and addresses
print ("Determining collection assignments ...")
with arcpy.da.SearchCursor(parcels, [idField, ownField]) as cursor:
    for row in cursor:
        if x in xList:
            duration = time.time()-start
            if x == 1:
                pass
            else:
                print("   cumulative duration: " + str(round(duration/60.0,1)) + " minutes")
            print("Working on parcel number " + str(x))
##            print ("Working on parcel ID: " + str(focId))

        #Define variables for fields
        focId = int(row[0])
        if row[1] is None:
            focOwn = ""
        elif row[1].replace(" ","") == "":
            focOwn = ""
        else:
            focOwn = re.sub(' +', ' ',row[1].replace(","," ").replace("."," ").replace(";"," ").replace("-"," ").replace(":"," ").strip())

        #If  the current focal parcel owner name is blank, give it a unique collection id and move on
        ##(it's not possible to proceed with neighbor analysis because there is no name and no address)
        if focOwn == "":
            parColl[focId] = collId
            collPar[collId] = [focId]
            collId+=1

        #If the current focal parcel owner name is NOT blank, retrieve list of neighboring parcels
        else:
            nghbrIds = nghbrDict[focId] ##nghbrDict = {focId: [nearId1, nearId2, nearId3 ...] ...}

            #Create empty list for storing near features that have similar names
            currColl = list()

            #iterate through neighbors in the nghbrIds
            for nearId in nghbrIds:

                #If the current near parcel ID is the same as the current focal parcel ID, pass as this is identifying the focal polygon in the parcel layer
                if nearId == focId:
                    pass

                #If the current near parcel ID is NOT the same as the current focal parcel ID, retrieve the owner name and address for the current near parcel ID from the ownership dictionary (ownDict)
                else:
                    nearOwn = ownDict[nearId]  #ownDict = {PARC_ID: PARC_OWN ...}

                    #If the current near parcel owner is blank, pass as we cannot aggregate it based on ownership
                    if nearOwn == "":
                        pass

                    #If the current near parcel owner name is NOT blank, check to see if fuzzystring matching of the names is feasible
                    else:

                        #if either the lengtt of the focOwn or nearOwn are 0, pass and don't perform the length ratio test as it would throw a divide by zero error and don't compare the strings
                        if len(focOwn) == 0 or len(nearOwn) == 0:
                            ownScore = 0

                        #find the ratio between lengths of the focal parcel owner name and current near parcel owner name (longer/shorter) to see if the strings should be compared
                        else:
                            ownDiff = max([len(focOwn),len(nearOwn)])/min([len(focOwn),len(nearOwn)])

                            #if the difference between the owner length ratio is >= string comparison length (comLen) threshold specified above, set the owner score to 0 as the lengths are too dissimilar to meaningfully compare the strings
                            if ownDiff >= compLen:
                                ownScore = 0

                            #if the difference between the owner length ratio is < the comparison length threshold specified above, compare the strings
                            else:
                                ownSort = fuzz.partial_token_sort_ratio(focOwn, nearOwn)
                                ownRatio = fuzz.partial_ratio(focOwn, nearOwn)

                                ownScore = max([ownSort, ownRatio])

                        #If the owner score is greater than the specified owner threshold score, or the address score is greater than the specified address threshold score, proceed with adding the near parcel id to the current collection list
                        if ownScore > ownThresh:
                            currColl.append(nearId)

                        #If neither the owner nor address scores are not greater than the specified thresholds, pass
                        else:
                            pass

            #if the current collection list has one or more entries (one or more neighbors), check to see if any of the collection members have already been assigned to a collection
            if len(currColl) > 0:

##                print ("neighbors of focal parcel (currColl):")
##                print (currColl)
                #create a new empty list for storing ids that have already been assigned to members of the current collection
                colls = list()

                for currMem in currColl:
                    #check to see if the current collection member has already been assigned to a collection in parColl
                    if currMem in parColl:
                        #if it has, retrieve the collection number and add it to the collections list
                        currCollId = parColl[currMem]
                        colls.append(currCollId)

                colls = list(set(colls))

                #if the collections list is empty (no members of the current collection have already been assigned a collection id), proceed with assigning the focal parcel and all members of the collection to a new collection id
                if len(colls) == 0:
##                    print ("none of the neighbors have already been assigned to a collection - new collection number assigned:")
##                    print (collId)
                    parColl[focId] = collId
                    collPar[collId] = [focId]

                    for currMem in currColl:
                        parColl[currMem] = collId
                        if collId in collPar:
                            if currMem in collPar[collId]:
                                pass
                            else:
                                collPar[collId].append(currMem)
                        else:
                            collPar[collId] = [currMem]
                        collId+=1

                #if the collection list has one or more items (members of the collection have already been assigned to one or more collection id), retrieve the smallest collection id that has already been used and assign all members of the collection to this collection id
                else:
##                    print ("neighbors have already been assigned to other collection(s) of ids:")
##                    print (colls)

                    #set up new list for collecting collection ids of other members of the collection(s)
                    otherColls = list()

                    #get collection numbers of all members of these other collection(s)
                    for coll in colls:
                        members = collPar[coll]
                        for member in members:
                            other = parColl[member]
                            otherColls.append(other)

                    #retrieve the smallest collection id that has already been used
                    assignedColl = min(colls)

                    #assign the focal parcel to this collection id
                    parColl[focId] = assignedColl
                    if focId in collPar[assignedColl]:
                        pass
                    else:
                        collPar[assignedColl].append(focId)

                    #assign all previously identified members of the collection to this collection id
                    for currMem in currColl:
                        parColl[currMem] = assignedColl
                        if currMem in collPar[assignedColl]:
                            pass
                        else:
                            collPar[assignedColl].append(currMem)


                    #retrieve all members that have been assigned to the other collection ids and assign their members to the smallest collection id (called assignedColl)
                    for other in colls:
                        if other == assignedColl:
                            pass
                        else:
                            needReassigned = collPar[other] #this is the list of members that have already been assigned to the current other collection
                            for reassign in needReassigned:
                                parColl[reassign] = assignedColl
                                if reassign in collPar[assignedColl]:
                                    pass
                                else:
                                    collPar[assignedColl].append(reassign)

                            collPar.pop(other)


            #If the current collection list is empty (the focal parcel has no neighbors), proceed with assigning it a new collection id
            elif len(currColl) == 0:
                parColl[focId] = collId
                collPar[collId] = [focId]
##                print ("the current parcel has no neighbors, assigning new collection id:")
##                print (collId)
                collId+=1
        x+=1

print ("Writing results to attribute table ...")
with arcpy.da.UpdateCursor(parcels, [idField, collField]) as upCursor:
    for upRow in upCursor:
        currParc = upRow[0]
        currParcColl = parColl[currParc]
        upRow[1] = currParcColl
        upCursor.updateRow(upRow)

finalStop = time.time()
finalElapsed = round(finalStop - initialStart, 1)
print ('All done!')
print ("  took " + str(finalElapsed/60.0) + " mins")




