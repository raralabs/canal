###Issue1
#####location: message.go
**Method**
---
    - func (m *Msg) AsBytes() ([]byte, error) 
**description**
---
    -The function encodes message using gob encoder
    In previous version of project it seems that 
    all the fields of the message were exported fields
    but now the fields are unexported which cause
    the method to malfunction gob encoder requires
    exported fields to work on.
    
**solution**
---
    - Has been implemented on file message.go
    under the same function.
    - Needs review to finalize it

 **Reference Taken From**
 ---
  ahref=https://riptutorial.com/go/example/14194/marshaling-structs-with-private-fields