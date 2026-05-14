"""
Simple script to add tabs to admin UI.
This script will insert tab CSS, HTML, and JavaScript into the clean server file.
"""
import re

# Read the clean version
with open('provider_coordinator_server_fresh.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Insert tab CSS after .section-stack style
section_stack_pattern = r'(\.section-stack \{\{\s*display: grid;\s*gap: 1rem;\s*\}\})'
section_stack_replacement = r'''\1

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
'''
content = re.sub(section_stack_pattern, section_stack_replacement, content)

# 2. Insert tab buttons after summary div
summary_div_pattern = r'(    </div>\n    <div class="grid">)'
tab_buttons_replacement = r'''\1

    <div class="tabs">
      <button class="tab active" data-tab="accounts">Accounts</button>
      <button class="tab" data-tab="blocks">Blocks</button>
      <button class="tab" data-tab="leases">Leases</button>
      <button class="tab" data-tab="events">Events</button>
      <button class="tab" data-tab="audit">Audit</button>
    </div>'''
content = re.sub(summary_div_pattern, tab_buttons_replacement, content)

# 3. Wrap Accounts section in tab-content div
# Find: <div class="grid"> after tab buttons and wrap it
accounts_grid_pattern = r'(    <div class="grid">)'
accounts_grid_replacement = r'<div id="accounts" class="tab-content active">\n    \1'
content = re.sub(accounts_grid_pattern, accounts_grid_replacement, content)

# 4. Close Accounts tab-content div before "Active Blocks" section
blocks_start_pattern = r'(      </div>\n    </div>\n    <div class="panel">\s*<h2>Active Blocks</h2>)'
blocks_start_replacement = r'      </div>\n    </div>\n    </div> <!-- Close accounts tab-content -->\n\n    <div class="panel">\n      <h2>Active Blocks</h2>'
content = re.sub(blocks_start_pattern, blocks_start_replacement, content)

# 5. Wrap Blocks section in tab-content div
blocks_section_pattern = r'(<div class="panel">\s*<h2>Active Blocks</h2>)'
blocks_section_replacement = r'<div id="blocks" class="tab-content">\n    \1'
content = re.sub(blocks_section_pattern, blocks_section_replacement, content)

# 6. Close Blocks tab-content div before "Runtime / Lease" section
leases_start_pattern = r'(      </div>\n    </div>\n    <div class="panel">\s*<h2>Runtime / Lease</h2>)'
leases_start_replacement = r'      </div>\n    </div>\n    </div> <!-- Close blocks tab-content -->\n\n    <div class="panel">\n      <h2>Runtime / Lease</h2>'
content = re.sub(leases_start_pattern, leases_start_replacement, content)

# 7. Wrap Leases section in tab-content div
leases_section_pattern = r'(<div class="panel">\s*<h2>Runtime / Lease</h2>)'
leases_section_replacement = r'<div id="leases" class="tab-content">\n    \1'
content = re.sub(leases_section_pattern, leases_section_replacement, content)

# 8. Close Leases tab-content div before "Recent Events" section
events_start_pattern = r'(      </div>\n    </div>\n    <div class="panel">\s*<h2>Recent Events</h2>)'
events_start_replacement = r'      </div>\n    </div>\n    </div> <!-- Close leases tab-content -->\n\n    <div class="panel">\n      <h2>Recent Events</h2>'
content = re.sub(events_start_pattern, events_start_replacement, content)

# 9. Wrap Events section in tab-content div
events_section_pattern = r'(<div class="panel">\s*<h2>Recent Events</h2>)'
events_section_replacement = r'<div id="events" class="tab-content">\n    \1'
content = re.sub(events_section_pattern, events_section_replacement, content)

# 10. Close Events tab-content div before "Admin Audit" section
audit_start_pattern = r'(      </div>\n    </div>\n    <div class="panel">\s*<h2>Admin Audit</h2>)'
audit_start_replacement = r'      </div>\n    </div>\n    </div> <!-- Close events tab-content -->\n\n    <div class="panel">\n      <h2>Admin Audit</h2>'
content = re.sub(audit_start_pattern, audit_start_replacement, content)

# 11. Wrap Audit section in tab-content div
audit_section_pattern = r'(<div class="panel">\s*<h2>Admin Audit</h2>)'
audit_section_replacement = r'<div id="audit" class="tab-content">\n    \1'
content = re.sub(audit_section_pattern, audit_section_replacement, content)

# 12. Close Audit tab-content div before </body>
# Find: </div> </div> </div> (closing divs for shell and body)
audit_end_pattern = r'(      </div>\n    </div>\n  </div>\n</body>\n</html>)'
audit_end_replacement = r'      </div>\n    </div>\n    </div> <!-- Close audit tab-content -->\n  </div>\n</body>\n</html>'
content = re.sub(audit_end_pattern, audit_end_replacement, content)

# 13. Update @media query to handle tabs
media_query_pattern = r'(@media \(max-width: 1100px\) \{\{\s*\.grid \{\{ grid-template-columns: 1fr; \}\}\})'
media_query_replacement = r'''\1
      .tabs {{ flex-wrap: wrap; }}
      .tab {{ flex: 1 1 auto; text-align: center; }}
'''''
content = re.sub(media_query_pattern, media_query_replacement, content)

# 14. Insert JavaScript before </body>
js_code = '''
    <script>
      document.addEventListener('DOMContentLoaded', function() {{
        const tabs = document.querySelectorAll('.tab');
        const tabContents = document.querySelectorAll('.tab-content');

        tabs.forEach(tab => {{
          tab.addEventListener('click', function() {{
            const targetTab = this.dataset.tab;

            tabs.forEach(t => t.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));

            this.classList.add('active');
            const targetContent = document.getElementById(targetTab);
            if (targetContent) {{
              targetContent.classList.add('active');
            }}
          }});
        }});

        // Auto-switch to appropriate tab based on URL hash
        if (window.location.hash) {{
          const hash = window.location.hash.substring(1);
          const targetTab = document.querySelector(`.tab[data-tab="${{hash}}"]`);
          const targetContent = document.getElementById(hash);
          if (targetTab && targetContent) {{
            tabs.forEach(t => t.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));
            targetTab.classList.add('active');
            targetContent.classList.add('active');
          }}
        }}
      }});
    </script>
'''
# Find: </body> and insert before it
content = re.sub(r'\n</body>', js_code + '\n</body>', content)

# Write the new file
with open('provider_coordinator_server_with_tabs.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Tabbed version written to provider_coordinator_server_with_tabs.py")
