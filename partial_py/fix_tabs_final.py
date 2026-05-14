import re

with open('provider_coordinator_server_remote.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix Blocks section - wrap in tab-content div
# Find: <div class="panel"> followed by <h2>Active Blocks</h2>
# Replace with: <div id="blocks" class="tab-content"> <div class="panel">

blocks_pattern = r'(<div class="panel">\s*<h2>Active Blocks</h2>)'
blocks_replacement = r'<div id="blocks" class="tab-content">\n      \1'
content = re.sub(blocks_pattern, blocks_replacement, content)

# 2. Fix Leases section
leases_pattern = r'(<div class="panel">\s*<h2>Runtime / Lease</h2>)'
leases_replacement = r'<div id="leases" class="tab-content">\n      \1'
content = re.sub(leases_pattern, leases_replacement, content)

# 3. Fix Events section
events_pattern = r'(<div class="panel">\s*<h2>Recent Events</h2>)'
events_replacement = r'<div id="events" class="tab-content">\n      \1'
content = re.sub(events_pattern, events_replacement, content)

# 4. Fix Audit section
audit_pattern = r'(<div class="panel">\s*<h2>Admin Audit</h2>)'
audit_replacement = r'<div id="audit" class="tab-content">\n      \1'
content = re.sub(audit_pattern, audit_replacement, content)

# 5. Close each tab-content div before the next section
# After Blocks table, close div before Leases
blocks_end_pattern = r'(      </div>\n    </div>\n    <div class="panel">\s*<h2>Runtime / Lease</h2>)'
blocks_end_replacement = r'      </div>\n    </div>\n    </div> <!-- Close blocks tab-content -->\n\n    <div class="panel">\n      <h2>Runtime / Lease</h2>'
content = re.sub(blocks_end_pattern, blocks_end_replacement, content)

# After Leases table, close div before Events
leases_end_pattern = r'(      </div>\n    </div>\n    <div class="panel">\s*<h2>Recent Events</h2>)'
leases_end_replacement = r'      </div>\n    </div>\n    </div> <!-- Close leases tab-content -->\n\n    <div class="panel">\n      <h2>Recent Events</h2>'
content = re.sub(leases_end_pattern, leases_end_replacement, content)

# After Events table, close div before Audit
events_end_pattern = r'(      </div>\n    </div>\n    <div class="panel">\s*<h2>Admin Audit</h2>)'
events_end_replacement = r'      </div>\n    </div>\n    </div> <!-- Close events tab-content -->\n\n    <div class="panel">\n      <h2>Admin Audit</h2>'
content = re.sub(events_end_pattern, events_end_replacement, content)

# 6. Close the last tab-content div (Audit) before the script
# Find: </div> <!-- Close audit tab-content -->
# If missing, add it before <script>
# Actually, let's check if we already have the script closing

with open('provider_coordinator_server_fixed_tabs.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed tabs version written to provider_coordinator_server_fixed_tabs.py")
