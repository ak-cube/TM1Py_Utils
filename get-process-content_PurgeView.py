from TM1py import TM1Service
from TM1py.Utils import Utils
import re
from copy import copy


##########
## Const
##########

result_file = "C:\\temp\\HardCode_PurgeView.txt"

warning_file = "C:\\temp\\HardCode_for_Analys.txt"

debug_file = "C:\\temp\\Debug.txt"

error_file = "C:\\temp\\Debug.txt"

search_list = ['VIEWZEROOUT', 'VIEWEXIST','VIEWDESTROY','VIEWCREATE','VIEWSUBSETASSIGN','SUBSETEXISTS','SUBSETDESTROY','SUBSETCREATE','SUBSETCREATEBYMDX']

source_folder = "C:\\temp\\src"

updated_folder = "C:\\temp\\new"

############
## Template
############

Header_str ="""

##############################################################################################################			
### CHANGE HISTORY:			
### MODIFICATION DATE 	CHANGED BY 	          COMMENT	
### 2022-10-01          Aleksandr Kolchugin   Prolog tab. CABE-5558	- Improvements [Update Bedrock] 
### YYYY-MM-DD 		      Developer Name 	      Reason for modification here : 
##############################################################################################################			
#Region @DOC			
# DESCRIPTION:			
# 			
# DATA SOURCE:			
#			
# USE CASE:			
#			
# NOTES:			
#		
# WARNING:			
# 			
#EndRegion			
##############################################################################################################	

######################
### Inits - declare constants

# Standard naming convention for source and target cubes/dimensions
sThisProcName = GetProcessName();
cCubParam       = '}APQ Settings';
sTimeStamp      = TimSt( Now, '\Y\m\d\h\i\s' );
sRandomInt      = NumberToString( INT( RAND( ) * 1000 ));
cViewSrcPrefix  = CellGetS( cCubParam, 'Std Datasource View Prefix', 'String' );
cViewSrc        = cViewSrcPrefix | sThisProcName |'_'| sTimeStamp |'_'| sRandomInt;
cSubSrc         = cViewSrc;
cViewClrPrefix  = CellGetS( cCubParam, 'Std ZeroOut View Prefix', 'String' );
cViewClr        = cViewClrPrefix | sThisProcName |'_'| sTimeStamp |'_'| sRandomInt;
cSubClr         = cViewClr;
cDebugLoc       = CellGetS( cCubParam, 'Location: Debugging', 'String' );
cDebugLoc       = cDebugLoc | IF( SubSt( cDebugLoc, Long( cDebugLoc ), 1 ) @<> '\\', '\\', '' );
cDebugFile      = cDebugLoc | sThisProcName |'_'| sTimeStamp;
sDebugFile      = cDebugFile | '_Prolog.log';
"""




def debug_dictionary_to_file(dic, open_type='w'):
    debugfile = open( debug_file, open_type)
    for (key, value) in dic.items() :
        debugfile.write('%s;%s\n' % (key,value))
    debugfile.close

def get_value (VarName, s_text):
    VarName = VarName.strip()
    if  VarName[0] =="'":
        ValValue =VarName[1:-1]
        Desc = "Hardcode"
    else:
        pattern_varaible = '(' + VarName + '\s*=\s*([^\;]+)\s*;)'
        all_var = re.findall(pattern_varaible, s_text)
        if len(all_var) == 0: 
            # print( VarName, "Can't find variable set" )
            ValValue = "warning"
            Desc = VarName + " Can't find variable set"
        elif len(all_var) > 1:
            ValValue = "error"
            # print( VarName, "More then 1 variable set" )
            Desc = VarName + " More then 1 variable set"
            for s_v_name in all_var:
                Desc += s_v_name[0]
        else:
            for s_v_name in all_var:
                    #print (proc.name, s_reg[0], s_reg[1].strip(), s_v_name[1], s_v_name[0])
                    if  s_v_name[1].strip()[0] =="'"and s_v_name[1].strip()[-1] =="'":
                        ValValue = s_v_name[1].strip()[1:-1]
                        Desc = s_v_name[0]
                    else:
                        ValValue = "warning"
                        Desc =  VarName + " is not a constant: " + s_v_name[0]
                    
    return [ValValue, Desc]

def get_text_createview(sCube, filter_string, zero={1|0}):

    Source_View = f"""

### Create Source View ###

sFilter = '{filter_string}';

ExecuteProcess( '{'}'}bedrock.cube.view.create', 
    'pLogOutput', 0,
    'pStrictErrorHandling', 1,
    'pCube', {sCube}, 
    'pView', 'cViewSrc', 
    'pFilter', sFilter,
    'pSuppressZero', 1, 
    'pSuppressConsol', 1, 
    'pSuppressRules', 1, 
    'pSuppressConsolStrings', 1,
    'pDimDelim', '&', 'pEleStartDelim', '¦', 'pEleDelim', '+',
    'pTemp', 1, 
    'pSubN', 0
);
 
######################
### Assign data source

DataSourceType          = 'VIEW';
DataSourceNameForServer = {sCube};
DatasourceCubeView      = 'cViewSrc';

    """

    Zero_View = f"""

### View ZeroOut ###

sFilter = '{filter_string}';

ExecuteProcess( '{'}'}bedrock.cube.data.clear',
    'pLogOutput', 0,
    'pStrictErrorHandling', 1,
    'pCube', {sCube}, 
    'pFilter', sFilter,
    'pSuppressConsolStrings', 1,
    'pDimDelim', '&', 'pEleStartDelim', '¦', 'pEleDelim', '+',
    'pCubeLogging', 0,
    'pTemp', 1, 
    'pSubN', 0
);
    """

    if zero: 
        New_Code = Zero_View
    else:
        New_Code = Source_View

    return (New_Code)

 
with TM1Service(address="localhost", port=8090, ssl=True, user="admin", password="") as tm1:
    
    ### Creating the list of processes fulfilled the criteria: name of process + consists the key words
    View_proc = {}
    Other_proc = {}
    for proc in tm1.processes.get_all():
        if "}APQ." in proc.name or "}bedrock." in proc.name or "}tp_" in proc.name or "Bedrock" in proc.name or "_backup_" in proc.name : continue
        Proc_tabs = {"PROLOG" : proc.prolog_procedure,
                    "METADATA" : proc.metadata_procedure,
                    "DATA" : proc.data_procedure,
                    "EPILOG" : proc.epilog_procedure }
       
        for tab_name in Proc_tabs.keys():
            text_data = Proc_tabs[tab_name]
            ### PROLOG
            if tab_name == "PROLOG":
                for s_word in ['VIEWCREATE']:
                    if  text_data.find(s_word) >0:
                        View_proc[proc.name] = s_word
                        if text_data.find('VIEWZEROOUT') >0: View_proc[proc.name] = View_proc[proc.name] + ';' + 'VIEWZEROOUT'
                        elif not proc.datasource_view: Other_proc[proc.name] = s_word + ';' + 'not a VIEW datasource'
                        if text_data.find('SUBSETCREATEBYMDX') >0: Other_proc[proc.name] = s_word + ';' + 'SUBSETCREATEBYMDX'
                    else:
                        for s_word in search_list:
                            if  text_data.find(s_word) >0:
                                if proc.name in Other_proc.keys():
                                    Other_proc[proc.name] = Other_proc[proc.name] + ';' + s_word
                                else:
                                    Other_proc[proc.name] = s_word
            else:
                for s_word in search_list:
                    if  text_data.find(s_word) >0:
                        if proc.name in Other_proc.keys():
                            Other_proc[proc.name] = Other_proc[proc.name] + ';' + 'METADATA:'+s_word
                        else:
                            Other_proc[proc.name] = tab_name + ':'+s_word
                             
    
    ### Exclude error processes from result dictionary
    View_proc = { x: View_proc[x] for x in View_proc if x not in Other_proc.keys() }

    print( len(View_proc) )
    # debug_dictionary_to_file(View_proc)
    debug_dictionary_to_file({'===Other processes=====':'===================='}, 'w')
    debug_dictionary_to_file(Other_proc, 'a')
    



    dic_hardcode = {}
    dic_error = {}
    i=0
    err_file = open( warning_file, 'w')
    
    #################
    ### Parsing process Prolog page
    for proc_name in View_proc.keys():
        i +=1
        # if not( proc_name == "CE ALL Actuals Load Incremental"): continue
        proc = tm1.processes.get(proc_name)
        text_data = proc.prolog_procedure
        
        #### Parsing VIEWCREATE - Cube, View

        patterns = ["(?imx)VIEWCREATE\s*\(([^\,]+),\s*([^\)]+)\s*\)" ]
        # patterns = [ "(?im)(SUBSETCREATEBYMDX)\s*\(([^\,]+),.*\)" ]
        for pattern_all in patterns: 
            all_ZeroOut = re.findall(pattern_all, text_data)
            if len(all_ZeroOut) == 0: print( i, proc.name, "VIEWCREATE","Nothing to match" )
            for s_regCubeView in all_ZeroOut:
                sCube = s_regCubeView[0].strip()
                sView = s_regCubeView[1].strip()
                [CubeName, CubeDesc] = get_value (sCube, text_data)
                [ViewName, ViewDesc] = get_value (sView, text_data)
                # print(i, proc.name, "cube = ", CubeName, "view = ", ViewName)
                if ViewName =="error":
                    err_line = "" + str(i) + " " + proc.name + ": " + ViewDesc; 
                    err_file.write(err_line + '\n')
                    dic_error[proc.name] = ["Num: " + str(i), "VIEW <-> " + ViewDesc]
                    break
        
                #### Parsing VIEWSUBSETASSIGN - View, Dimension, Subset
        
                    # dic_hardcode[proc.name] = {"Cube": [CubeName, CubeDesc], "View": [ViewName, ViewDesc], "Dim":{}}
                patt_assign = "(?im)VIEWSUBSETASSIGN\s*\(\s*"+sCube+"\s*,\s*"+sView+"\s*,\s*([^\,]+),\s*([^\)]+)\s*\)"
                all_Assing = re.findall(patt_assign, text_data)
                # print( i, proc.name, all_Assing)
                dim_dic = {}
                for s_reg in all_Assing:
                    sDim = s_reg[0].strip()
                    sSub = s_reg[1].strip()
                    [DimName, DimDesc] = get_value (sDim, text_data)
                    [SubName, SubDesc] = get_value (sSub, text_data)
                    if DimName =="error":
                        err_line = "" + str(i) + " " + proc.name + ": " + DimDesc 
                        err_file.write(err_line + '\n')
                        dic_error[proc.name] = ["Num: " + str(i), "DIMENSION <-> " + DimDesc]
                        break

                    #### Parsing SUBSETELEMENTINSERT - Dimenision, Subset, Element
        
                    patt_sub = "(?im)SUBSETELEMENTINSERT\s*\(\s*"+sDim+"\s*,\s*"+sSub+"\s*,\s*([^\,]+),[^\)]+\)"
                    all_subins = re.findall(patt_sub, text_data)
                    if len(all_subins) ==0: 
                        err_line = "" + str(i) + " " + proc.name + ": " + ViewName +"<->" + DimName + " - There's no Element Insert" 
                        err_file.write(err_line + '\n')
                        dic_error[proc.name] = ["Num: " + str(i), "SUBSETELEMENTINSERT <-> " + DimName +"<->" + sSub]
                    Set_of_Elements = []
                    for sElem in all_subins:
                        [ElemName, ElDesc] = get_value (sElem, text_data)
                        if ElemName =="error" or ElemName =="warning":
                            err_line = "" + str(i) + " " + proc.name + ": " + ElDesc
                            err_file.write(err_line + '\n')
                            dic_error[proc.name] = ["Num: " + str(i), "ELEMENT <-> " + ElDesc]
                        else:
                            # if tm1.elements.exists(DimName,DimName,ElemName):
                            Set_of_Elements.append(ElemName)
                    dim_dic[DimName] = {"Name":DimName,"Sub":SubName, "Elem":Set_of_Elements}
                
                ### Writing the result to dictionary
                
                dic_hardcode[proc.name] = {"Num":i, "Cube": [sCube, CubeName, CubeDesc], "View": [ViewName, ViewDesc], "Dim":dim_dic, "text":text_data}

### Exclude error processes from result dictionary
result_dic = { x: dic_hardcode[x] for x in dic_hardcode if x not in dic_error.keys() }

debug_dictionary_to_file({'===Error processes=====':'===================='}, 'a')
debug_dictionary_to_file(dic_error, 'a')

View_proc = { x: View_proc[x] for x in View_proc if x not in dic_error.keys() }
debug_dictionary_to_file({'===Automated processes=====':'===================='}, 'a')
debug_dictionary_to_file(View_proc, 'a')


print(len(result_dic))
print(len(dic_error))


with TM1Service(address="localhost", port=8090, ssl=True, user="admin", password="") as tm1:
    for proc in tm1.processes.get_all():
        if proc.name not in result_dic.keys() : continue
        text_data = proc.prolog_procedure
        proc_name = proc.name
#    for proc_name in result_dic.keys():
        # if not( proc_name == "CE ALL COMMENTS Load Comments"): continue
        # print(proc_name)
 #       text_data = result_dic[proc_name]["text"]
        p_first = len(text_data) 
        p_last = 0

        ### Create sFilter string ## 'pDimDelim', '&', 'pEleStartDelim', '¦', 'pEleDelim', '+'
        sCube = result_dic[proc_name]["Cube"][0]
        sFilter_string = ""
        for DimName in result_dic[proc_name]["Dim"].keys():
            if len(sFilter_string)==0:
                sFilter_string = DimName + '¦'
            else:
                sFilter_string = sFilter_string + '&' + DimName + '¦'
            sFilter_string = sFilter_string + '+'.join(result_dic[proc_name]["Dim"][DimName]["Elem"])
        
        ############
        ### Find region for replacment
        for s_word in search_list:
            if p_first>text_data.find(s_word)>0: p_first = text_data.find(s_word)
            if p_last<text_data.rfind(s_word): p_last = text_data.rfind(s_word)
        p_first = text_data.rfind('\n',0,p_first) + 1
        p_last =  text_data.find(';',p_last) + 1     

        if "VIEWZEROOUT" in View_proc[proc_name]: 
            New_text = get_text_createview(sCube, sFilter_string, 1)
        else:
            New_text = get_text_createview(sCube, sFilter_string, 0)
        
 


        old_file = open(source_folder + '\\' + proc_name + '.txt', 'w')
        old_file.write(text_data)
        old_file.close

        resut_text = text_data[:p_first] + New_text + text_data[p_last:]
        pos = resut_text.find("#****End: Generated Statements****")
        pos = resut_text.find('\n', pos)
        resut_text = resut_text[:pos] + Header_str + resut_text[pos:]
        view_str = result_dic[proc_name]["View"][1] # -- delete the row with view defenition if it exists;
        resut_text = resut_text.replace(view_str,'')
  

        new_file = open(updated_folder + '\\' + proc_name + '.txt', 'w')
        new_file.write(resut_text )
        new_file.close

        ### Create a backup
        proc_new = copy(proc)
        proc_new.name = "_bkp_" + proc_name
        tm1.processes.update_or_create(proc_new)

        proc.prolog_procedure = resut_text
        tm1.processes.update(proc)
        


