sudo tar zxvf jdk*.tar.gz -C /usr/lib/jvm
cd  /usr/lib/jvm
sudo ln -s jdk*/ java8
vim /etc/profile

add text at the end of the file 
#java environment
export JAVA_HOME=/usr/lib/jvm/java8
export JRE_HOME=$JAVA_HOME/jre
export CLASSPATH=$JAVA_HOME/lib:$JRE_HOME/lib:$CLASSPATH
export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH

soure /etc/profile

sudo update-alternatives --install /usr/bin/java 	java 	/usr/lib/jvm/java8/bin/java 	300
sudo update-alternatives --install /usr/bin/javac 	javac 	/usr/lib/jvm/java8/bin/javac 	300
sudo update-alternatives --install /usr/bin/javah 	javah 	/usr/lib/jvm/java8/bin/javah 	300
sudo update-alternatives --install /usr/bin/jar 	jar 	/usr/lib/jvm/java8/bin/jar 	300