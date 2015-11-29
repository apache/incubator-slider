import re
from os import fsync

def updating(filename,dico):

    RE = '(('+'|'.join(dico.keys())+')\s*=)[^\r\n]*?(\r?\n|\r)'
    pat = re.compile(RE)

    def jojo(mat,dic = dico):
        return str(dic[mat.group(2)]).join(mat.group(1,3))

    with open(filename,'rb') as f:
        content = f.read() 

    with open(filename,'wb') as f:
        f.write(pat.sub(jojo,content))
