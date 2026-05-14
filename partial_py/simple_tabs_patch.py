# Simple script to add tabs to admin UI

with open('provider_coordinator_server.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

output_lines = []
in_summary = False
summary_closed = False
found_grid = False
in_style = False
style_indent = "    "

i = 0
while i < len(lines):
    line = lines[i]
    
    # Insert CSS for tabs after .muted style and before .grid style
    if '.muted { color: var(--muted); font-size: 0.88rem; }' in line:
        output_lines.append(line)
        # Add tab CSS
        output_lines.append(style_indent + '.tabs {\n')
        output_lines.append(style_indent + '  display: flex;\n')
        output_lines.append(style_indent + '  gap: 0.4rem;\n')
        output_lines.append(style_indent + '  margin: 1rem 0 0 0;\n')
        output_lines.append(style_indent + '  border-bottom: 2px solid var(--line);\n')
        output_lines.append(style_indent + '  padding-bottom: 0;\n')
        output_lines.append(style_indent + '}\n')
        output_lines.append(style_indent + '.tab {\n')
        output_lines.append(style_indent + '  border: 0;\n')
        output_lines.append(style_indent + '  border-radius: 12px 12px 0 0;\n')
        output_lines.append(style_indent + '  padding: 0.7rem 1.1rem;\n')
        output_lines.append(style_indent + '  font-size: 0.92rem;\n')
        output_lines.append(style_indent + '  font-weight: 600;\n')
        output_lines.append(style_indent + '  color: var(--muted);\n')
        output_lines.append(style_indent + '  background: rgba(255,255,255,0.5);\n')
        output_lines.append(style_indent + '  cursor: pointer;\n')
        output_lines.append(style_indent + '  transition: all 0.15s;\n')
        output_lines.append(style_indent + '  margin-bottom: -2px;\n')
        output_lines.append(style_indent + '  border: 2px solid transparent;\n')
        output_lines.append(style_indent + '}\n')
        output_lines.append(style_indent + '.tab:hover {\n')
        output_lines.append(style_indent + '  background: rgba(255,255,255,0.8);\n')
        output_lines.append(style_indent + '  color: var(--ink);\n')
        output_lines.append(style_indent + '}\n')
        output_lines.append(style_indent + '.tab.active {\n')
        output_lines.append(style_indent + '  color: var(--accent);\n')
        output_lines.append(style_indent + '  background: var(--paper);\n')
        output_lines.append(style_indent + '  border-color: var(--line);\n')
        output_lines.append(style_indent + '  border-bottom-color: var(--paper);\n')
        output_lines.append(style_indent + '}\n')
        output_lines.append(style_indent + '.tab-content {\n')
        output_lines.append(style_indent + '  display: none;\n')
        output_lines.append(style_indent + '  animation: fadeIn 0.2s ease;\n')
        output_lines.append(style_indent + '}\n')
        output_lines.append(style_indent + '.tab-content.active {\n')
        output_lines.append(style_indent + '  display: block;\n')
        output_lines.append(style_indent + '}\n')
        output_lines.append(style_indent + '@keyframes fadeIn {\n')
        output_lines.append(style_indent + '  from { opacity: 0; transform: translateY(4px); }\n')
        output_lines.append(style_indent + '  to { opacity: 1; transform: translateY(0); }\n')
        output_lines.append(style_indent + '}\n')
        i += 1
        continue

    # Insert tab buttons after summary closing div
    if '</div>' in line and 'summary' in lines[i-3:i+1] and not summary_closed:
        output_lines.append(line)
        # Add tab buttons
        output_lines.append('\n' + style_indent + '<div class="tabs">\n')
        output_lines.append(style_indent + '  <button class="tab active" data-tab="accounts">Accounts</button>\n')
        output_lines.append(style_indent + '  <button class="tab" data-tab="blocks">Blocks</button>\n')
        output_lines.append(style_indent + '  <button class="tab" data-tab="leases">Leases</button>\n')
        output_lines.append(style_indent + '  <button class="tab" data-tab="events">Events</button>\n')
        output_lines.append(style_indent + '  <button class="tab" data-tab="audit">Audit</button>\n')
        output_lines.append(style_indent + '</div>\n')
        summary_closed = True
        i += 1
        continue

    # Wrap Accounts section in tab-content
    if '<div class="grid">' in line and summary_closed and not found_grid:
        output_lines.append('    <div id="accounts" class="tab-content active">\n')
        output_lines.append(line)
        found_grid = True
        i += 1
        continue

    # After closing </div> for Accounts grid, close tab-content
    if '</div>\n    </div>\n' in line and found_grid and 'tab-content' not in ''.join(output_lines[-10:]):
        # Check if this is the end of the Accounts section (before other sections)
        # We'll add closing tab-content after the Accounts grid
        if 'Active Blocks' in ''.join(lines[i+1:i+3]):
            output_lines.append('    </div>\n')  # Close tab-content
            output_lines.append('\n' + style_indent + '<div id="blocks" class="tab-content">\n')

    # Wrap each subsequent section in its own tab-content
    if '<h2>Active Blocks</h2>' in line:
        output_lines.append(line)
        # Open blocks tab-content if not already opened
        if 'id="blocks"' not in ''.join(output_lines[-5:]):
            output_lines.append('    <div id="blocks" class="tab-content">\n')
        i += 1
        continue

    if '<h2>Runtime / Lease</h2>' in line:
        output_lines.append('    </div>\n')  # Close previous
        output_lines.append('\n' + style_indent + '<div id="leases" class="tab-content">\n')
        output_lines.append(line)
        i += 1
        continue

    if '<h2>Recent Events</h2>' in line:
        output_lines.append('    </div>\n')
        output_lines.append('\n' + style_indent + '<div id="events" class="tab-content">\n')
        output_lines.append(line)
        i += 1
        continue

    if '<h2>Admin Audit</h2>' in line:
        output_lines.append('    </div>\n')
        output_lines.append('\n' + style_indent + '<div id="audit" class="tab-content">\n')
        output_lines.append(line)
        i += 1
        continue

    output_lines.append(line)
    i += 1

# Write modified file
with open('provider_coordinator_server_tabs.py', 'w', encoding='utf-8') as f:
    f.writelines(output_lines)

print("Tabbed version written to provider_coordinator_server_tabs.py")
