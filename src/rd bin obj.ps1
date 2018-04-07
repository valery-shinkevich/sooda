gci -Path . -Recurse -Force -Include obj | rm -Force -Recurse -Verbose #-WhatIf 
gci -Path . -Recurse -Force -Include bin | rm -Force -Recurse -Verbose #-WhatIf
#gci -Path e:\NewProject\ -Recurse -Force -Include *.*scc | rm -Force 
#gci -Path e:\NewProject\ -Recurse -Force -Include *resharper* | rm -Force

