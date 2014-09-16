
import datetime

'''
New log schema:
{
    _id: mongo
    collection: string
    id: integer
    time: time
    user: user_id
    action (opt): string (item created, item deleted)
    data (opt): deleted object
    changes: [
        {
            <data fieldname>: old_value (non-object or list)
            <data fieldname>: {
                action: string     (field added, field deleted)
                data (opt): deleted value
            }
            <list fieldname>: {
                action: string     (<value> added, <value> deleted, order changed)
                data (opt): deleted object if <value> was object or [] of old order if order changed
            }
            <obj fieldname>: {
                action: string     (item created, item deleted)
                data (opt): deleted object
            }
        }
    ]
}


'''


def diff_recursive(new, old, path=''):
    'Object level'
    changes = []
    for key, val in new.items():
        if isinstance(val, dict):
            if key not in old:
                changes.append({path+key: {'action': 'field added'}})
            else:
                changes.extend(diff_recursive(val, old[key], key + '/'))
        elif isinstance(val, list):
            if key not in old:
                changes.append({path+key: {'action': 'field added'}})
            else:
                changes.extend(diff_lists_recursive(val, old[key], path+key))
        else:
            if key not in old:
                changes.append({path+key: {'action': 'field added'}})
            elif old[key] != val:
                changes.append({path+key: old[key]})
                
    for key, val in old.items():
        if key not in new:
            changes.append({path+key: {'action': 'field removed', 'data': val}})
                
    return changes
    
    
def diff_lists_recursive(new, old, path):
    changes = []
    
    if any(map(lambda x: isinstance(x, dict), new+old)):
        object_type = 'object'
    else:        
        object_type = 'item'

    def simple_val(x):
        if isinstance(x, list):
            raise TypeError, 'diff cannot handle 2D lists'
        elif isinstance(x, dict):
            global object_type
            object_type = 'object'
            return x['_id']
        else:
            return x

    oldvals = map(simple_val, old)
    newvals = map(simple_val, new)
    matched = []

    stack = list(newvals)
    for val in oldvals:
        if val in stack:
            matched.append(val)
            stack.remove(val)
            
    stack = list(matched)
    oldmatched = []
    for i, val in enumerate(oldvals):
        if val in stack:
            stack.remove(val)
            oldmatched.append(val)
        else:
            changes.append({path: {'action': '%s removed' % object_type, 'data': old[i]}})

    stack = list(matched)
    newmatched = []
    for i, val in enumerate(newvals):
        if val in stack:
            stack.remove(val)
            newmatched.append(val)
        else:
            changes.append({path: {'action': '%s added' % object_type, 'data': val}})
            
    if oldmatched != newmatched:
        changes.append({path: {'action': 'array reordered', 'data': oldmatched}})
        
    if object_type == 'object':
        for _id in matched:
            oldval = filter(lambda x: x['_id']==_id, old)[0]
            newval = filter(lambda x: x['_id']==_id, new)[0]
            ind = newvals.index(_id)
            changes.extend(diff_recursive(newval, oldval, '%s/%i/' % (path, ind)))


    return changes
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    