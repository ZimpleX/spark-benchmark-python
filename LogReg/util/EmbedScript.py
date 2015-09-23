"""
this module enables you to call shell script within python script, using submodule
"""

def runScript(script, stdin=None):
    """
    embed shell script into this python script:
        reference: http://stackoverflow.com/questions/2651874/embed-bash-in-python

    argument:
        script  string  the script to be executed via "bash -c"
        stdin   list    should be a list of arguments passed to the script
    return:
        stdout and stderr of script
    """
    import subprocess as sp
    proc = sp.Popen(['bash', '-c', script] + stdin,
        stdout=sp.PIPE, stderr=sp.PIPE, stdin=sp.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode:
        raise ScriptException(proc.returncode, stdout, stderr, script)
    return stdout, stderr

class ScriptException(Exception):
    def __init__(self, returncode, stdout, stderr, script):
        super(ScriptException, self).__init__("error in script")
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout
        self.script = script
    def __str__(self):
        return "EXCEPTION IN SCRIPT: \nerr msg: " + str(self.stderr) \
            + "\nstdout: " + str(self.stdout) \
            + "\nreturn code: " + str(self.returncode) \
            + "\noriginal script: \n****\n" + str(self.script)

