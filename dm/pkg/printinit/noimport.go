package printinit

// we expect this helper function to be called at very early stage of initialization, because this package has no
// imports.
// There are some failing CI indicating it takes at least 10 seconds to start a DM-master, we need to know if the OS
// spawn processes too slowly or some init() are slow to finish.
func init() {
	println("init now")
}
