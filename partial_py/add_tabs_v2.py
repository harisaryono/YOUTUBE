import re

# Read the original file
with open('provider_coordinator_server_fresh.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Replace the CSS in render_admin_page to add tab styles
# Find the specific CSS block and insert tab styles before @media
old_css_block = """    .section-stack {{
      display: grid;
      gap: 1rem;
    }}"""

new_css_block = """    .section-stack {{
      display: grid;
      gap: 1rem;
    }}
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
    }}"""

content = content.replace(old_css_block, new_css_block)

# 2. Find the start of the HTML return statement in render_admin_page
# We look for the specific pattern: return f"""<!doctype html>
html_start_marker = '    return f"""<!doctype html>'
if html_start_marker in content:
    # We will replace the entire HTML body from the summary end to the script
    # But we need to preserve the f-string variables (like {summary['total_accounts']})
    
    # Let's insert the tab buttons after the summary div
    summary_end_marker = """    </div>
    <div class="grid">"""
    
    tabs_html = """    </div>

    <div class="tabs">
      <button class="tab active" data-tab="accounts">Accounts</button>
      <button class="tab" data-tab="blocks">Blocks</button>
      <button class="tab" data-tab="leases">Leases</button>
      <button class="tab" data-tab="events">Events</button>
      <button class="tab" data-tab="audit">Audit</button>
    </div>

    <div id="accounts" class="tab-content active">
      <div class="grid">"""
    
    content = content.replace(summary_end_marker, tabs_html)
    
    # 3. Close the accounts tab-content div and open new ones for other sections
    # We need to find the end of the Accounts section (end of "section-stack" div)
    # and wrap other panels in tab-content divs.
    
    # Close the accounts tab-content after the Accounts section
    # The Accounts section ends with </div> </div> (grid and section-stack)
    # We need to inject closing div and opening div for Blocks
    
    # Find the specific pattern: </div> </div> </div> </div> </div> </div> </div>
    # This is hard to match reliably. Let's try matching the start of "Active Blocks" panel.
    
    # Pattern: <div class="panel"> followed by <h2>Active Blocks</h2>
    # Replace with closing tab-content and opening new one
    
    blocks_section_start = """      </div>
    </div>
    </div>
    <div class="panel">
      <h2>Active Blocks</h2>"""
    
    new_blocks_section_start = """      </div>
    </div>
    </div>
    </div> <!-- Close accounts tab-content -->

    <div id="blocks" class="tab-content">
      <div class="panel">
        <h2>Active Blocks</h2>"""
    
    content = content.replace(blocks_section_start, new_blocks_section_start)
    
    # Do the same for Leases
    leases_section_start = """      </div>
    </div>
    <div class="panel">
      <h2>Runtime / Lease</h2>"""
    
    new_leases_section_start = """      </div>
    </div>
    </div> <!-- Close blocks tab-content -->

    <div id="leases" class="tab-content">
      <div class="panel">
        <h2>Runtime / Lease</h2>"""
        
    content = content.replace(leases_section_start, new_leases_section_start)
    
    # Do the same for Events
    events_section_start = """      </div>
    </div>
    <div class="panel">
      <h2>Recent Events</h2>"""
    
    new_events_section_start = """      </div>
    </div>
    </div> <!-- Close leases tab-content -->

    <div id="events" class="tab-content">
      <div class="panel">
        <h2>Recent Events</h2>"""
    
    content = content.replace(events_section_start, new_events_section_start)
    
    # Do the same for Audit
    audit_section_start = """      </div>
    </div>
    <div class="panel">
      <h2>Admin Audit</h2>"""
    
    new_audit_section_start = """      </div>
    </div>
    </div> <!-- Close events tab-content -->

    <div id="audit" class="tab-content">
      <div class="panel">
        <h2>Admin Audit</h2>"""
    
    content = content.replace(audit_section_start, new_audit_section_start)
    
    # 4. Add closing div for audit tab-content before </body>
    # Find the closing of the last panel: </div> </div> </div> </div>
    # The structure is: <div class="shell"> ... <div class="tab-content"> <div class="panel"> ... </div> </div> </div>
    
    # We need to close the audit tab-content before closing the shell
    audit_end_marker = """      </div>
    </div>
  </div>
</body>
</html>\"\"\""""
    
    new_audit_end_marker = """      </div>
    </div>
    </div> <!-- Close audit tab-content -->
  </div>
</body>
</html>\"\"\""""
    
    content = content.replace(audit_end_marker, new_audit_end_marker)
    
    # 5. Add JavaScript for tab switching before </body>
    # We already modified the structure above, now we need to inject the script.
    # Let's inject it inside the shell div, after the audit tab-content.
    
    js_inject_marker = """    </div> <!-- Close audit tab-content -->
  </div>
</body>"""
    
    js_code = """    </div> <!-- Close audit tab-content -->

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
  </div>
</body>"""
    
    content = content.replace(js_inject_marker, js_code)

    # 6. Update @media query to handle tab wrapping
    media_query = """    @media (max-width: 1100px) {{
      .grid {{ grid-template-columns: 1fr; }}
    }}"""
    
    new_media_query = """    @media (max-width: 1100px) {{
      .grid {{ grid-template-columns: 1fr; }}
      .tabs {{ flex-wrap: wrap; }}
      .tab {{ flex: 1 1 auto; text-align: center; }}
    }}"""
    
    content = content.replace(media_query, new_media_query)

    with open('provider_coordinator_server_tabs.py', 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Tabbed version written to provider_coordinator_server_tabs.py")
else:
    print("ERROR: Could not find HTML start marker. File structure may have changed.")

