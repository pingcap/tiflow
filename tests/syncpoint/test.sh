
function deployConfig() {
    cat ./conf/diff_config_part1.toml > ./conf/diff_config.toml
    echo "snapshot = "$1"" >> ./conf/diff_config.toml
    cat ./conf/diff_config_part2.toml >> ./conf/diff_config.toml
    echo "snapshot = "$2"" >> ./conf/diff_config.toml
}
deployConfig 123 456
