"""
Fix JavaScript quotes to avoid Python f-string conflict.
Replace all double quotes with single quotes in JavaScript.
"""
import re

with open('provider_coordinator_server_with_tabs.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace JavaScript section with single quotes
# Find: <script> ... </script>
script_pattern = r'(<script>\s*document\.addEventListener\(\'DOMContentLoaded\', function\(\) \{\{\s*const tabs = document\.querySelectorAll\(\'\.tab\'\);\s*const tabContents = document\.querySelectorAll\(\'\.tab-content\'\);\s*tabs\.forEach\(tab => \{\{\s*tab\.addEventListener\(\'click\', function\(\) \{\{\s*const targetTab = this\.dataset\.tab;\s*tabs\.forEach\(t => t\.classList\.remove\(\'active\'\)\);\s*tabContents\.forEach\(tc => tc\.classList\.remove\(\'active\'\)\);\s*this\.classList\.add\(\'active\'\);\s*const targetContent = document\.getElementById\(targetTab\);\s*if \(targetContent\) \{\{\s*targetContent\.classList\.add\(\'active\'\);\s*\}\}\s*\}\);\s*\}\);\s*// Auto-switch to appropriate tab based on URL hash\s*if \(window\.location\.hash\) \{\{\s*const hash = window\.location\.hash\.substring\(1\);\s*const targetTab = document\.querySelector\(`\.tab\[data-tab="\$\{hash\}"\`);\s*const targetContent = document\.getElementById\(hash\);\s*if \(targetTab && targetContent\) \{\{\s*tabs\.forEach\(t => t\.classList\.remove\(\'active\'\)\);\s*tabContents\.forEach\(tc => tc\.classList\.remove\(\'active\'\)\);\s*targetTab\.classList\.add\(\'active\'\);\s*targetContent\.classList\.add\(\'active\'\);\s*\}\s*\}\s*\}\);\s*</script>)'

new_script = '''<script>
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
    </script>'''

content = content.replace(script_pattern, new_script)

with open('provider_coordinator_server_fixed_js.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed JavaScript quotes")
