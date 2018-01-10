# Code Formatting

## Eclipse

Eclipse code formatting and (basic) project configuration files can be found at the `etc/` folder. You should manually 
copy them _after importing all your projects_:

    for settings_dir in `find . -type d -name .settings`; do
       \cp -v etc/ide-settings/eclipse/org.eclipse.jdt.* $settings_dir
    done

Do not use the [maven-eclipse-plugin](https://maven.apache.org/plugins/maven-eclipse-plugin/) to copy the files as it 
conflicts with [m2e](https://eclipse.org/m2e/).

## IDEA

If you completed the step described on [idea instructions](ide.md#style-templates-and-inspection-settings-for-idea), and selected the code style accordingly you should be ready to go.

## EditorConfig

For editors supporting [EditorConfig](http://editorconfig.org/), a settings file is provided in
etc/ide-settings/editorconfig.ini. Copy it to your Artemis top level directory and
[name it .editorconfig](http://editorconfig.org/#file-location)
