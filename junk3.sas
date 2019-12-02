data junk3; junk3='junk5'; run;

options dlcreatedir notes source;

data a;
 version = gitfn_version();
 put version=;             
 run;

 data b;
 rc = gitfn_clone("https://github.com/SeacoastBank/hello-world/",
   "c:\helloworld4");
 put rc=;
run;