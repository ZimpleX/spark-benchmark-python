"""
this module enables you to call shell script within python script, using submodule
"""
from functools import reduce

def runScript(script, stdin=None, output_opt='pipe', input_opt='pipe', input_pipe=['']):
    """
    embed shell script into this python script:
        reference: http://stackoverflow.com/questions/2651874/embed-bash-in-python

    argument:
        script  string  the script to be executed via "bash -c"
        stdin   list    should be a list of arguments passed to the script
        output_opt  string  how should the output be treated: whether to pipe it or to display it directly in terminal
    return:
        stdout and stderr of script
        input_pipe  the list of string that you want to pipe to subprocess as input
    """
    import subprocess as sp
    proc = None
    if output_opt == 'pipe':
        output_opt = sp.PIPE
    else:
        output_opt = None
    if input_opt == 'pipe':
        input_opt = sp.PIPE
        input_pipe = "\n".join(input_pipe).encode('utf-8')
    else:
        input_opt = None
        input_pipe = None

    proc = sp.Popen(['bash', '-c', script] + stdin,
        stdout=output_opt, stderr=output_opt, stdin=input_opt)
    
    stdout, stderr = proc.communicate(input=input_pipe)

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

