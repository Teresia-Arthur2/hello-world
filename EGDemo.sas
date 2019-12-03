**************************************************************;
*** Author:  Teresia Arthur                                ***;
*** Date:    8AUG19                                        ***;
*** Purpose: Demonstrate how to interact with Viya from EG ***;
**************************************************************;
*** Step 1: Create a connection to sasoa with userid/pw    ***;
*** server: sasoa.corp.local                               ***;
*** port: 8561                                             ***;
*** store login in profile                                 ***;

*** BEGIN EXAMPLE 1 INTERACTING VIYA WITH EG ***;

*** Create a cas session ***;
cas mySession 
host="sasdevcontrollerrhelsr01.corp.local"
port=5570 
sessopts=(caslib=casuser timeout=1800 locale="en_US")
;

*** Allocate the global caslibs ***;
caslib _all_ assign;

*** Show me a list of the global caslibs I have access to ***;
caslib _all_ list;

*** Allocate the vdw library ***;
libname vdwlib odbc datasrc='vdw_dsn' user='sasreader' password="&vdwprod" schema=vdw  preserve_col_names=yes preserve_tab_names=yes DBLIBINIT='SET queryTimeout TO 0'; 
  
*** Allocate a library on sas datasets on sasoa ***;
libname mylib "D:/analytics/SAS Datasets/Master/Account/Analysis" access=readonly;
libname orig "D:/analytics/SAS Datasets/Master/Customer" access=readonly;

libname geo "D:/analytics/SAS Datasets/Master/Geo" access=readonly;

*** COpy a table from sasoa to a caslib ***;
libname tla cas caslib=tlatemp;
proc sql noprint;
  create table tla.cltv_pred_horiz_master as
  select *
  from orig.cltv_pred_horiz_master
;quit;


proc sql ;
describe table geo.usm;
quit;


*** Read a caslib table the old way ***;
proc sql noprint;
  create table work.junk as
  select *
  from dmo.dv_active_directory_user(datalimit=all);
quit;

*** Read a caslib table using the new cas to do the work ***;
proc fedsql sessref=mysession;
  create table casuser.junk2 as
  select *
  from dmo.dv_active_directory_user;
quit;

*** Read a sas dataset located on SASOA ***;
data casuser.calendar;
  set mylib.calendar_stacky_cpa;
run;

*** BEGIN EXAMPLE 2: LOADING SAS DATASET INTO VIYA USING EG ***;

*** Load a sas dataset into memory in caslib dmotest ***;
%let mytable = calendar_stacky_cpa;
%let myincaslib = dmotest;
%let myoutcaslib = dmotest;
 
*** These particular steps are prescribed by sas tech support  ***;
*** in order to deflate the bloat we see when loading sas7bdat ***;
*** VARCHARCONVERSION=column-length                            ***;
*** specifies the column length at which to begin converting   ***;
*** CHAR to VARCHAR. Specify a number that is either greater   ***;
*** than or equal to 1 (>=1) or that is less than or equal to  ***;
*** 32767 (<= 32767).                                          ***;
proc casutil incaslib=&myincaslib outcaslib=&myoutcaslib;
    *** drop tables in case they already exist ***;
    droptable casdata="&mytable" incaslib="&myincaslib" quiet;
    droptable casdata="&mytable.1" incaslib="&myincaslib" quiet;
    droptable casdata="&mytable" incaslib="&myoutcaslib" quiet;
    droptable casdata="&mytable.1" incaslib="&myoutcaslib" quiet;

    *** load 1 ***; 
    load data=mylib.&mytable casout="&mytable.1"; 
    *** creata a sas7bdat ***;
    save casdata="&mytable.1" casout="&mytable" replace exportOptions=(filetype="basesas") ;  
     *** load 2 with varcharconversion ***;
    load casdata="&mytable" casout="&mytable.2" importOptions=(filetype="basesas", varcharconversion=8); 
    *** Create a compressed sashdat ***;
    save casdata="&mytable.2" casout="&mytable" compress replace  ;  
    *** load 3 and promote from hdat to memory  ***;
    load casdata="&mytable..sashdat"   casout="&mytable"  PROMOTE TRANSCODE_FAIL=WARN;  
    *** delete and drop the files/tables we do not need ***;
    deletesource casdata="&mytable..sas7bdat" incaslib="&myincaslib";
    droptable casdata="&mytable.2" incaslib="&myincaslib" quiet;
    droptable casdata="&mytable.1" incaslib="&myincaslib" quiet; 
    *** List the files/tables that remain ***;
    list files;
    list tables;
    contents casdata = "&mytable" ;
quit;

*** BEGIN EXAMPLE 3: LOADING A TABLE FROM VDW TO VIYA USING EG ***;

*** Load and compress a table from vdw straight into memory in caslib dmotest ***;
%let myin = vdw;
%let myout = dmotest;
%let mydataset = dv_active_directory_user;
%let mywhere = ;

*** load the staging data                                                 ***;
*** Example of mywhere syntax:                                            ***;
*** mywhere = 'options={where="full_dte >= ' || "'2019-07-01'" || '"}';   ***;
proc casutil incaslib = "&myin" outcaslib = "&myout" ;
  load casdata = "&mydataset" casout = "&mydataset._stg" copies = 0 replace &mywhere;
quit; 

*** Save to a compressed hdat file so data can autoload when needed ***;
 proc casutil incaslib = "&myout" outcaslib = "&myout";
   *** Save to a compressed hdat file ***;
   save casdata = "&mydataset._stg" casout = "&mydataset" replace compress;
   *** Drop superfluous data ***;
   droptable casdata = "&mydataset._stg" quiet;
   droptable casdata = "&mydataset" quiet;
   *** Load from the compress hdat ***;
   load casdata="&mydataset..sashdat" casout="&mydataset"  PROMOTE TRANSCODE_FAIL=WARN; 
   *** Show some information about our files/tables ***;
   list files;
   list tables;
   contents casdata = "&mydataset";
quit;

*** Clean up after these demos ***;
proc casutil incaslib = "dmotest" outcaslib = "dmotest";
  droptable casdata = "calendar_stacky_cpa" quiet;
  droptable casdata = "dv_active_directory_user" quiet;
  droptable casdata = "dv_cdq_kqi_summary" quiet;
  deletesource casdata = "calendar_stacky_cpa.sashdat" quiet;
  deletesource casdata = "dv_active_directory_user.sashdat" quiet;
  deletesource casdata = "dv_cdq_kqi_summary.sashdat" quiet;
  list files;
  list tables;
quit;

proc fedsql sessref=mysession;
  drop table casuser.calendar;
  drop table casuser.junk2;
quit;

/*******************
*** Find old tables and files that we want to purge ***;

  proc cas;
    table.tableInfo result=results quiet=true caslib = "dmo";
    if results then 
      saveresult results dataout=work.results;
  run;

  proc cas;
    table.fileInfo result=results allFiles=True caslib = "dmo";
    if results then 
      saveresult results dataout=work.fileresults;
  run;

  quit; 

data work.fileresults2;
  set work.fileresults;
  length date 8;
  format date date9.;
  date = datepart(modtime);
  if date < today()-100 then output;
run;

data work.results2;
  set work.results;
  length date 8;
  format date date9.;
  date = datepart(modtime);
  if date < today()-100 then output;
run;
****************/

*** Create a cas session ***;
cas mySession 
host="sasdevcontrollerrhelsr01.corp.local"
port=5570 
sessopts=(caslib=casuser timeout=1800 locale="en_US")
;

*** Allocate the global caslibs ***;
caslib _all_ assign;

*** Show me a list of the global caslibs I have access to ***;
caslib _all_ list;

*** allocate library on sasoa ***;
libname mylib "D:/analytics/SAS Datasets/Master/Account/Analysis" access=readonly;

proc copy in=mylib out=tlatemp ;
run;