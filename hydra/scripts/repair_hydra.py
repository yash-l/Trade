import os

file = os.path.expanduser("~/hydra/core/module5.py")

with open(file,"r") as f:
    code = f.read()

# patch USER_KEY error
code = code.replace(
"client.USER_KEY",
"getattr(client,'USER_KEY',None)"
)

code = code.replace(
"self.client.USER_KEY",
"getattr(self.client,'USER_KEY',None)"
)

# prevent crash if credentials missing
patch = """

# HYDRA AUTO PATCH
if not hasattr(self.client,"USER_KEY"):
    self.client.USER_KEY = None
"""

if "HYDRA AUTO PATCH" not in code:
    code = code.replace("FivePaisaClient(", patch + "\nFivePaisaClient(")

with open(file,"w") as f:
    f.write(code)

print("Hydra broker patch applied")
