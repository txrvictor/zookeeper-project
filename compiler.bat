set ZK="C:\temp\zookeeper-3.4.9"
set CP_ZK=.;%ZK%\zookeeper-3.4.9.jar;%ZK%\lib\slf4j-log4j12-1.6.1.jar;%ZK%\lib\slf4j-api-1.6.1.jar;%ZK%\lib\log4j-1.2.16.jar
@echo ***** Compiling
javac -cp %CP_ZK% *.java
@echo ***** Execute
java -cp %CP_ZK% -Dlog4j.configuration=file:%ZK%\conf\log4j.properties TokenGenerator localhost

