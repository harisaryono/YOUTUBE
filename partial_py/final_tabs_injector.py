"""
Final script to add tabs to admin UI.
This script will replace the entire HTML body in render_admin_page
with a tabbed version.
"""

import re

# Read the clean version
with open('provider_coordinator_server_v3.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the start of HTML return statement in render_admin_page
# Pattern: return f"""<!doctype html>
html_start = '    return f"""<!doctype html>\n'
if html_start not in content:
    print("ERROR: Could not find HTML start marker")
    exit(1)

# We need to replace from html_start to the end of the function
# Find the end of the function: next 'def ' or end of file

# Let's find the index of html_start
html_start_idx = content.find(html_start)
if html_start_idx == -1:
    print("ERROR: Could not find HTML start index")
    exit(1)

# Find the end of the function (next 'def ' after render_admin_page)
next_def_match = re.search(r'\n\ndef [^\n]+\(', content[html_start_idx + 10:])
if next_def_match:
    next_def_idx = html_start_idx + 10 + next_def_match.start()
else:
    next_def_idx = len(content)

# Extract the HTML f-string template
old_html_template = content[html_start_idx:next_def_idx]

# Now we'll build the new HTML template with tabs
# We need to keep all the f-string variables intact

# Parse the old HTML to extract the key variables
# We'll look for specific patterns and preserve them

# New tabbed HTML template
# We'll construct it using string manipulation to preserve f-string syntax
new_html_template = """    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Provider Coordinator Admin</title>
  <style>
    :root {{
      --bg: #efe8de;
      --paper: #fffdf8;
      --ink: #1e2528;
      --muted: #69757a;
      --line: #d5cabc;
      --accent: #0a6c74;
      --good: #0b6e4f;
      --warn: #9a6700;
      --danger: #9b2226;
      --shadow: 0 18px 50px rgba(22, 24, 28, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(10,108,116,0.14), transparent 28rem),
        radial-gradient(circle at top right, rgba(11,110,79,0.10), transparent 22rem),
        linear-gradient(180deg, #f7f1e8 0%, var(--bg) 100%);
    }}
    .shell {{
      width: min(1500px, calc(100% - 2rem));
      margin: 1rem auto 2rem;
    }}
    .topbar {{
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: flex-start;
      padding: 1.2rem 1.4rem;
      background: rgba(255, 253, 248, 0.86);
      border: 1px solid rgba(213, 202, 188, 0.92);
      border-radius: 20px;
      backdrop-filter: blur(10px);
      box-shadow: var(--shadow);
    }}
    .topbar h1 {{ margin: 0; font-size: 1.7rem; }}
    .muted {{ color: var(--muted); font-size: 0.88rem; }}
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
    .summary {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 0.8rem;
      margin-top: 1rem;
    }}
    .stat {{
      background: rgba(255,255,255,0.72);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 0.9rem 1rem;
    }}
    .stat strong {{
      display: block;
      font-size: 1.5rem;
      margin-top: 0.25rem;
    }}
    label {{
      display: block;
      margin-bottom: 0.8rem;
      font-size: 0.9rem;
      font-weight: 600;
    }}
    input, textarea, select {{
      width: 100%;
      margin-top: 0.35rem;
      padding: 0.72rem 0.85rem;
      border: 1px solid var(--line);
      border-radius: 12px;
      font: inherit;
      background: #fff;
      color: var(--ink);
    }}
    textarea {{ min-height: 110px; resize: vertical; }}
    .button-row {{
      display: flex;
      gap: 0.6rem;
      flex-wrap: wrap;
      margin-top: 1rem;
    }}
    button, .link-button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 40px;
      border: 0;
      border-radius: 12px;
      padding: 0.68rem 0.95rem;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      text-decoration: none;
      background: var(--accent);
      color: #fff;
    }}
    button.small, .link-button {{
      min-height: 34px;
      padding: 0.45rem 0.72rem;
      font-size: 0.88rem;
    }}
    button.good {{ background: var(--good); }}
    button.danger {{ background: var(--danger); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.92rem;
    }}
    th, td {{
      text-align: left;
      vertical-align: top;
      padding: 0.7rem 0.55rem;
      border-bottom: 1px solid rgba(213, 202, 188, 0.9);
    }}
    th {{
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
    }}
    .notes {{
      max-width: 30rem;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    .state {{
      display: inline-block;
      padding: 0.2rem 0.55rem;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }}
    .state-idle {{ background: rgba(10,108,116,0.10); color: var(--accent); }}
    .state-busy {{ background: rgba(154,103,0,0.13); color: var(--warn); }}
    .state-blocked {{ background: rgba(155,34,38,0.12); color: var(--danger); }}
    .state-disabled, .state-error {{ background: rgba(64,64,64,0.12); color: #404040; }}
    .tag {{
      display: inline-block;
      margin-left: 0.35rem;
      padding: 0.14rem 0.44rem;
      font-size: 0.72rem;
      border-radius: 999px;
      background: rgba(11,110,79,0.12);
      color: var(--good);
    }}
    .actions {{
      display: flex;
      gap: 0.4rem;
      flex-wrap: wrap;
    }}
    .banner {{
      border-radius: 14px;
      padding: 0.8rem 0.95rem;
      margin-top: 1rem;
      border: 1px solid transparent;
    }}
    .banner-ok {{
      background: rgba(11,110,79,0.10);
      color: var(--good);
      border-color: rgba(11,110,79,0.20);
    }}
    .banner-error {{
      background: rgba(155,34,38,0.10);
      color: var(--danger);
      border-color: rgba(155,34,38,0.20);
    }}
    .subtle {{
      margin-top: 0.5rem;
      font-size: 0.86rem;
      color: var(--muted);
    }}
    .section-stack {{
      display: grid;
      gap: 1rem;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 360px minmax(0, 1fr);
      gap: 1rem;
      margin-top: 1rem;
    }}
    .panel {{
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 1rem;
      box-shadow: var(--shadow);
    }}
    .panel h2 {{
      margin: 0 0 0.8rem;
      font-size: 1.08rem;
    }}
    @media (max-width: 1100px) {{
      .grid {{ grid-template-columns: 1fr; }}
      .tabs {{ flex-wrap: wrap; }}
      .tab {{ flex: 1 1 auto; text-align: center; }}
    }}
    @media (max-width: 640px) {{
      .shell {{ width: min(100% - 1rem, 100%); }}
      .topbar {{ flex-direction: column; }}
      table,thead, tbody, th, td, tr {{ display: block; }}
      thead {{ display: none; }}
      tr {{
        border-bottom: 1px solid rgba(213, 202, 188, 0.9);
        padding: 0.35rem 0;
      }}
      td {{
        border: 0;
        padding: 0.28rem 0;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div>
        <h1>Provider Coordinator Admin</h1>
        <div class="muted">DB: {h(db_path)}</div>
        <div class="subtle">Pantau akun, lease, block, dan error provider tanpa edit SQLite manual.</div>
      </div>
      <form method="post" action="/admin/logout">
        <button type="submit" class="small">Logout</button>
      </form>
    </div>
    {flash_html}
    {error_html}
    <div class="summary">
      <div class="stat"><div class="muted">Total akun</div><strong>{summary['total_accounts']}</strong></div>
      <div class="stat"><div class="muted">Akun aktif</div><strong>{summary['active_accounts']}</strong></div>
      <div class="stat"><div class="muted">Akun nonaktif</div><strong>{summary['inactive_accounts']}</strong></div>
      <div class="stat"><div class="muted">Akun diblock</div><strong>{summary['blocked_accounts']}</strong></div>
      <div class="stat"><div class="muted">Lease aktif</div><strong>{summary['leased_accounts']}</strong></div>
      <div class="stat"><div class="muted">Event dimuat</div><strong>{summary['event_count']}</strong></div>
      <div class="stat"><div class="muted">Audit dimuat</div><strong>{summary['audit_count']}</strong></div>
    </div>

    <div class="tabs">
      <button class="tab active" data-tab="accounts">Accounts</button>
      <button class="tab" data-tab="blocks">Blocks</button>
      <button class="tab" data-tab="leases">Leases</button>
      <button class="tab" data-tab="events">Events</button>
      <button class="tab" data-tab="audit">Audit</button>
    </div>

    <div id="accounts" class="tab-content active">
      <div class="grid">
        <div class="panel">
          <h2>{h(selected_title)}</h2>
          <form method="post" action="/admin/accounts/save">
            <input type="hidden" name="provider_account_id" value="{form_values['provider_account_id']}">
            <label>Provider
              <input name="provider" value="{h(form_values['provider'])}" required>
            </label>
            <label>Account name
              <input name="account_name" value="{h(form_values['account_name'])}" required>
            </label>
            <label>Model default
              <input name="model_name" value="{h(form_values['model_name'])}" required>
            </label>
            <label>Endpoint URL
              <input name="endpoint_url" value="{h(form_values['endpoint_url'])}" required>
            </label>
            <label>Usage method
              <input name="usage_method" value="{h(form_values['usage_method'])}" required>
            </label>
            <label>API key
              <input name="api_key" type="password" value="" {'required' if not form_values['provider_account_id'] else ''}>
            </label>
            <div class="subtle">Kosongkan saat edit bila API key tidak ingin diganti.</div>
            <label>Extra headers JSON
              <textarea name="extra_headers_json">{h(form_values['extra_headers_json'])}</textarea>
            </label>
            <label>Notes
              <textarea name="notes">{h(form_values['notes'])}</textarea>
            </label>
            <label>
              <select name="is_active">
                <option value="1" {'selected' if form_values['is_active'] else ''}>Active</option>
                <option value="0" {'selected' if not form_values['is_active'] else ''}>Inactive</option>
              </select>
            </label>
            <div class="button-row">
              <button type="submit" class="good">Simpan akun</button>
              {'<button type="submit" formaction="/admin/accounts/test">Test koneksi</button>' if form_values['provider_account_id'] else ''}
              <a class="link-button" href="/admin">Reset form</a>
            </div>
          </form>
        </div>
        <div class="section-stack">
          <div class="panel">
            <h2>Filter</h2>
            <form method="get" action="/admin">
              <div class="button-row">
                <label style="flex:2 1 320px;">Cari akun
                  <input name="q" value="{h(filters['search_query'])}" placeholder="provider, nama akun, model, endpoint, notes">
                </label>
                <label style="flex:1 1 180px;">Provider
                  <input name="provider" value="{h(filters['provider_filter'])}" placeholder="mis. z.ai">
                </label>
                <label style="flex:1 1 160px;">State
                  <select name="state">
                    <option value="" {'selected' if not filters['state_filter'] else ''}>Semua</option>
                    <option value="idle" {'selected' if filters['state_filter'] == 'idle' else ''}>idle</option>
                    <option value="in_use" {'selected' if filters['state_filter'] == 'in_use' else ''}>in_use</option>
                    <option value="blocked" {'selected' if filters['state_filter'] == 'blocked' else ''}>blocked</option>
                    <option value="disabled" {'selected' if filters['state_filter'] == 'disabled' else ''}>disabled</option>
                  </select>
                </label>
                <label style="flex:2 1 280px;">Cari event/audit
                  <input name="event_q" value="{h(filters['event_query'])}" placeholder="event_type, message, payload">
                </label>
              </div>
              {f'<input type="hidden" name="account_id" value="{form_values["provider_account_id"]}">' if form_values['provider_account_id'] else ''}
              <div class="button-row">
                <button type="submit">Terapkan filter</button>
                <a class="link-button" href="/admin">Reset filter</a>
              </div>
            </form>
          </div>
          <div class="panel">
            <h2>Accounts</h2>
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Account</th>
                  <th>Model</th>
                  <th>Status</th>
                  <th>Active</th>
                  <th>Blocked until</th>
                  <th>Last event</th>
                  <th>Reason / notes</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>{accounts_rows_html}</tbody>
            </table>
          </div>
          <div class="panel">
            <h2>Status per Model</h2>
            <table>
              <thead>
                <tr>
                  <th>Model</th>
                  <th>Runtime</th>
                  <th>Blocked until</th>
                  <th>Holder</th>
                  <th>Last event</th>
                  <th>Reason / note</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>{model_rows_html}</tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <div id="blocks" class="tab-content">
      <div class="panel">
        <h2>Active Blocks</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Account</th>
              <th>Model</th>
              <th>Blocked until</th>
              <th>Reason</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>{blocks_rows_html}</tbody>
        </table>
      </div>
    </div>

    <div id="leases" class="tab-content">
      <div class="panel">
        <h2>Runtime / Lease</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Account</th>
              <th>Model</th>
              <th>State</th>
              <th>Holder</th>
              <th>Lease expires</th>
              <th>Note</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>{leases_rows_html}</tbody>
        </table>
      </div>
    </div>

    <div id="events" class="tab-content">
      <div class="panel">
        <h2>Recent Events</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Account</th>
              <th>Model</th>
              <th>Event</th>
              <th>At</th>
              <th>Payload</th>
            </tr>
          </thead>
          <tbody>{events_rows_html}</tbody>
        </table>
      </div>
    </div>

    <div id="audit" class="tab-content">
      <div class="panel">
        <h2>Admin Audit</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Action</th>
              <th>Target</th>
              <th>Actor</th>
              <th>At</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>{audit_rows_html}</tbody>
        </table>
      </div>
    </div>

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
</body>
</html>\"\"\""""

# Now replace the old HTML template with the new one
new_content = content[:html_start_idx] + new_html_template + content[next_def_idx:]

with open('provider_coordinator_server_v4.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("Tabbed version v4 written to provider_coordinator_server_v4.py")
