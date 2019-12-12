data junk3; junk3='junk7'; run;

options dlcreatedir notes source;

data a;
 version = gitfn_version();
 put version=;             

 rc = gitfn_clone("https://github.com/SeacoastBank/hello-world/",
   "c:\helloworld4");
 put rc=;
run;
*comment2*;
*comment3*;
*comment4*;
