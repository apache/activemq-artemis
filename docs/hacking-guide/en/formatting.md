# Code Formatting

Eclipse code formatting and (basic) project configuration files can be found at the `etc/` folder. You should manually 
copy them _after importing all your projects_:

    for settings_dir in `find . -type d -name .settings`; do
       \cp -v etc/org.eclipse.jdt.* $settings_dir
    done

Do not use the [maven-eclipse-plugin](https://maven.apache.org/plugins/maven-eclipse-plugin/) to copy the files as it 
conflicts with [m2e](http://eclipse.org/m2e/).