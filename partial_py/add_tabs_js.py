with open('provider_coordinator_server_tabs.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find </body> and add script before it
js_code = '''
    <script>
      document.addEventListener('DOMContentLoaded', function() {
        const tabs = document.querySelectorAll('.tab');
        const tabContents = document.querySelectorAll('.tab-content');

        tabs.forEach(tab => {
          tab.addEventListener('click', function() {
            const targetTab = this.dataset.tab;

            tabs.forEach(t => t.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));

            this.classList.add('active');
            const targetContent = document.getElementById(targetTab);
            if (targetContent) {
              targetContent.classList.add('active');
            }
          });
        });

        // Auto-switch to appropriate tab based on URL hash
        if (window.location.hash) {
          const hash = window.location.hash.substring(1);
          const targetTab = document.querySelector(`.tab[data-tab="${hash}"]`);
          const targetContent = document.getElementById(hash);
          if (targetTab && targetContent) {
            tabs.forEach(t => t.classList.remove('active'));
            tabContents.forEach(tc => tc.classList.remove('active'));
            targetTab.classList.add('active');
            targetContent.classList.add('active');
          }
        }
      });
    </script>'''

# Insert before </body>
content = content.replace('</body>', js_code + '\n</body>')

with open('provider_coordinator_server_tabs.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("JavaScript for tabs added")
