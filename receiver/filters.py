"""MQTT主题过滤匹配：单层+、末尾多层#与$系统主题规则。"""

def _levels(name):
    if not isinstance(name, str) or name == '':
        raise ValueError('主题不能为空')
    return name.split('/')

def matches(filter_name, topic_name):
    """按MQTT规则判断单个过滤器是否匹配主题。

    - “+”恰好匹配一个层级；“#”只能作为最后一层，匹配剩余任意层级（含父层级）。
    - 以“#”或“+”开头的通配过滤器不匹配“$”系统主题，显式过滤器仍可匹配。
    """
    f = _levels(filter_name)
    t = _levels(topic_name)
    for i, level in enumerate(f):
        if level == '#':
            if i != len(f) - 1:
                raise ValueError('多层通配符#只能位于过滤器末尾')
            if i == 0 and t[0].startswith('$'):
                return False
            return True
        if i >= len(t):
            return False
        if level == '+':
            if i == 0 and t[0].startswith('$'):
                return False
            continue
        if level != t[i]:
            return False
    return len(f) == len(t)

def accepted(filters, topic_name):
    """主题命中任一配置过滤器才在采集范围内。"""
    return any(matches(f, topic_name) for f in filters)
