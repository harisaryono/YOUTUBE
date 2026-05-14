import re

# Read original file
with open('provider_coordinator_server.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Read new tab-based HTML template (we'll create it inline)
new_tab_html = """
    <div class="tabs">
      <button class="tab active" data-tab="accounts">Accounts</button>
      <button class="tab" data-tab="blocks">Blocks</button>
      <button class="tab" data-tab="leases">Leases</button>
      <button class="tab" data-tab="events">Events</button>
      <button class="tab" data-tab="audit">Audit</button>
    </div>
"""

# Insert tabs after summary div, before grid div
pattern = r'(    </div>\n    <div class="grid">)'
replacement = r'    </div>\n' + new_tab_html.strip() + '\n    <div class="grid">'
content = re.sub(pattern, replacement, content)

# Insert CSS for tabs
# Find the end of .grid style and add tab styles before @media
tab_css = """
    .tabs {{
      display: flex;
      gap: 0.4rem;
      margin: 1rem 0 0 0;
      border-bottom: 2px solid var(--line);
      padding-bottom: 0;
    }}
    .tab {{
      border: 0;
      border-radius: 12px 12px 0 0;
      padding: 0.7rem 1.1rem;
      font-size: 0.92rem;
      font-weight: 600;
      color: var(--muted);
      background: rgba(255,255,255,0.5);
      cursor: pointer;
      transition: all 0.15s;
      margin-bottom: -2px;
      border: 2px solid transparent;
    }}
    .tab:hover {{
      background: rgba(255,255,255,0.8);
      color: var(--ink);
    }}
    .tab.active {{
      color: var(--accent);
      background: var(--paper);
      border-color: var(--line);
      border-bottom-color: var(--paper);
    }}
    .tab-content {{
      display: none;
      animation: fadeIn 0.2s ease;
    }}
    .tab-content.active {{
      display: block;
    }}
    @keyframes fadeIn {{
      from {{ opacity: 0; transform: translateY(4px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
"""

css_pattern = r'(@media \(max-width: 1100px\) \{\{)'
css_replacement = tab_css.strip() + '\n\n' + css_pattern
content = re.sub(css_pattern, css_replacement, content)

# Now we need to wrap each section in tab-content divs
# This is complex. Instead, we'll wrap the entire "section-stack" div in a tab-content for accounts
# And add separate tab-content divs for other sections

# Find the opening of each panel section and wrap appropriately
# For simplicity, we'll add JS that handles tab switching by showing/hiding sections
# We'll add IDs to each panel section first

# Add ID to Accounts form panel
content = re.sub(
    r'(<div class="panel">\s*<h2>\{h\(selected_title\)\}</h2>)',
    r'<div id="accounts-form" class="panel">\2',
    content
)

# Add ID to Accounts table panel
content = re.sub(
    r'(<div class="section-stack">\s*<div class="panel">\s*<h2>Filter</h2>)',
    r'<div id="accounts-list" class="section-stack">\2',
    content
)

# For now, let's just write the file and test
with open('provider_coordinator_server_tabs.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("File written to provider_coordinator_server_tabs.py")
