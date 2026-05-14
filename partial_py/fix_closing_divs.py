# Add missing closing divs for tab-content wrappers

with open('provider_coordinator_server_fixed_tabs.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

output = []
i = 0
while i < len(lines):
    line = lines[i]
    output.append(line)
    
    # After blocks panel closes, add closing div for tab-content
    if '<tbody>{blocks_rows_html}</tbody>' in line:
        # Skip ahead to find </table> and </div> for panel
        j = i + 1
        while j < len(lines) and '</table>' not in lines[j]:
            j += 1
        # After </table>, find </div> that closes the panel
        while j < len(lines) and '</div>' not in lines[j]:
            j += 1
        # Found closing </div> for panel, add closing div for tab-content after it
        if j < len(lines):
            output.append('    </div> <!-- Close blocks tab-content -->\n')
    
    # After leases panel closes
    if '<tbody>{leases_rows_html}</tbody>' in line:
        j = i + 1
        while j < len(lines) and '</table>' not in lines[j]:
            j += 1
        while j < len(lines) and '</div>' not in lines[j]:
            j += 1
        if j < len(lines):
            output.append('    </div> <!-- Close leases tab-content -->\n')
    
    # After events panel closes
    if '<tbody>{events_rows_html}</tbody>' in line:
        j = i + 1
        while j < len(lines) and '</table>' not in lines[j]:
            j += 1
        while j < len(lines) and '</div>' not in lines[j]:
            j += 1
        if j < len(lines):
            output.append('    </div> <!-- Close events tab-content -->\n')
    
    i += 1

with open('provider_coordinator_server_fixed_tabs_v2.py', 'w', encoding='utf-8') as f:
    f.writelines(output)

print("Fixed closing divs")
