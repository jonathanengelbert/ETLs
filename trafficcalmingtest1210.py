import glob, os
tempfolder = "\\\\CP-GIS-SVR1\\arcgisserver\\DataAndMXDs\\TIMStaging\\zip\\TC\\"
for filename in os.listdir(tempfolder):
    print filename

newstring = "TC_1"
    
for filename in os.listdir(tempfolder):
    exten = os.path.splitext(filename) # filename and extensionname (extension in [1])
    print exten[1]
    os.rename(tempfolder + filename, tempfolder + newstring + exten[1])