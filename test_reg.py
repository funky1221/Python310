# v1.0 test
import re

mystring=\
"""<p>This is a paragraph</p>
xxxxxxxxxte
<p>This is alsot
a random paragraph</p>
"""

pattern = re.findall("</p>", mystring)

print(pattern)