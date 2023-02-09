from types import SimpleNamespace

from yamlpath.common import Parsers
from yamlpath.wrappers import ConsolePrinter
from yamlpath import Processor


# The various classes of this library must be able to write messages somewhere
# when things go bad.  This project provides a CLI-centric logging class named
# ConsolePrinter.  Even when not writing a CLI tool, you must still configure
# and pass ConsolePrinter or a class of your own with the same public API.  For
# just muting logging output -- except for unrecoverable errors -- you can use
# this simple configuration object:
logging_args = SimpleNamespace(quiet=True, verbose=False, debug=False)
log = ConsolePrinter(logging_args)

# Prep the YAML parser and round-trip editor (tweak to your needs).  You do not
# have to use Parsers.get_yaml_editor() but you must create a properly-
# configured instance of ruamel.yaml.YAML.
yaml = Parsers.get_yaml_editor()

# At this point, you'd load or parse your YAML file, stream, or string.  This
# example demonstrates loading YAML data from an external file.  You could also
# use the same function to load data from STDIN or even a String variable.  See
# the Parser class for more detail.
yaml_file = "config.yaml"
(yaml_data, doc_loaded) = Parsers.get_yaml_data(yaml, log, yaml_file)
if not doc_loaded:
    # There was an issue loading the file; an error message has already been
    # printed via ConsolePrinter.
    exit(1)

# Pass the logging facility and parsed YAML data to the YAMLPath Processor
processor = Processor(log, yaml_data)

# At this point, the Processor is ready to handle YAML Paths
