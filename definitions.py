
class Definitions():
    
    controllerMap = { 
                        "node": {
                            "state/on": {"PowerController": ["powerState"] }, 
                            "state/bri": {"BrightnessController": ["brightness"] } 
                        }
    }

    
    commands = {}
    
    thermostatModesByName = { 'OFF':'0','HEAT':'1','COOL':'2','AUTO':'3','FAN':'4','PROGRAM HEAT':'5','PROGRAM COOL':'6','PROGRAM AUTO':'7' }       

    busyStates={
                    "0":"Not Busy",
                    '1':'Busy',
                    '2':'Idle',
                    '3':'Safe Mode'
    }

    triggerEvents={
                    '0':'Event Status',
                    '1':'Get Status',
                    '2':'Key Changed',
                    '3':'Info String',
                    '4':'IR Learn Mode',
                    '5':'Schedule Status Changed',
                    '6':'Variable Status Changed',
                    '7':'Variable Initialized',
                    'X':'Unknown'
    }
    
    nodeChanges={
                    'NN':'Node Renamed',
                    'NR':'Node Removed',
                    'ND':'Node Added',
                    'MV':'Node Moved (into a scene)',
                    'CL':'Link Changed (in a scene)',
                    'RG':'Removed From Group (scene)',
                    'EN':'Enabled',
                    'PC':'Parent Changed',
                    'PI':'Power Info Changed',
                    'DI':'Device ID Changed',
                    'DP':'Device Property Changed',
                    'GN':'Group Renamed',
                    'GR':'Group Removed',
                    'GD':'Group Added',
                    'FN':'Folder Renamed',
                    'FR':'Folder Removed',
                    'FD':'Folder Added',
                    'NE':'Node Error (Comm. Errors)',
                    'CE':'Clear Node Error (Comm. Errors Cleared)',
                    'SN':'Discovering Nodes (Linking)',
                    'SC':'Node Discovery Complete',
                    'WR':'Network Renamed',
                    'WH':'Pending Device Operation',
                    'WD':'Programming Device',
                    'RV':'Node Revised (UPB)'
    }
    
    deviceTypes={
                    "0.18.0.0":"button",
                    "0.17.0.0":"button",
                    "0.5.0.0":"button",
                    "5.11.11.0":"thermostat",
                    "1.9.43.0":"light",
                    "1.0.51.0":"light",
                    "1.1.53.0":"light",
                    "1.6.51.0":"light",
                    "1.14.58.0":"light",
                    "1.14.65.0":"light",
                    "1.25.56.0":"lightswitch",
                    "1.25.64.0":"lightswitch",
                    "1.28.57.0":"light",
                    "1.32.64.0":"lightswitch",
                    "1.32.65.0":"lightswitch",
                    "2.6.65.0":"light",
                    "2.9.0.0":"light",
                    "2.42.67.0":"lightswitch",
                    "2.56.67.0":"light",
                    "2.31.65.0":"device",
                    "3.13.0.0":"device",
                    "2.55.70.0":"device"
    }
    
    wirelessDeviceTypes=['0.5.0.0', '0.17.0.0', "0.18.0.0"]
        

