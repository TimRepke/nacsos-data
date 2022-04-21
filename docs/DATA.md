### User
* Type (human vs machine)
* Name 
* email
* plain-text password

### Item
* Text

### TweetItem
* Item (link to Item containing text)

### AbstractItem
* Item (link to Item containing abstract)
* 

### FullTextItem
* PDF (pointer to original file on disk)
* Item (link to Item containing extracted full-text; markdown?)
* AbstractItem (if available, link to abstract item)

### Project
* Type (Twitter, Abstracts, FullText)
* Name 
* Description (some text describing the project)

### Query
Note: is that a good idea? We will have several data sources, this will become inconsistent
* Name
* Type (wos, scopus, twitter)

### ItemTags
Note: do we need hierarchies?
Note: split document-level and text-level?
* AttributeName
* Values (values this attribute can take)


### TextTags
* AttributeName
* Values (values this attribute can take)

### Tasks
* Project
* Description
* Type (buttons, drop-down, free-text, free-text with suggestions)
* Attributes (jsonb)

### TaskAssignments
* Task
* User
* Item

### Annotations
* Time
* User
* Task
* Item
* Attribute
* Value
* (level)