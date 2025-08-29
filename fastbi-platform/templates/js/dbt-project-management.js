// Core functionality for project cards
document.addEventListener('DOMContentLoaded', function() {
    const grid = document.getElementById('project-grid');

    // Set all toggle-details buttons to have a transparent background by default
    const toggleButtons = document.querySelectorAll('.toggle-details');
    toggleButtons.forEach(button => {
        button.style.background = 'transparent'; // Set default background to transparent
    });

    // Close all option menus when clicking outside
    document.addEventListener('click', function(event) {
        const menus = document.querySelectorAll('.options-menu.active');
        menus.forEach(menu => {
            if (!menu.contains(event.target) &&
            !event.target.matches('.options-trigger')) {
                menu.classList.remove('active');
            }
        });
    });

    grid.addEventListener('click', function(event) {
        if (event.target.classList.contains('toggle-details') || event.target.closest('.toggle-details')) {
            ProjectConfig.initializeConfigButtons();
            const button = event.target.closest('.toggle-details');
            const projectCard = button.closest('.project-card');
            const projectId = projectCard.dataset.projectId;

            const details = projectCard.querySelector('.project-details');
            const icon = button.querySelector('i');

            // Toggle the active class
            details.classList.toggle('active');
            button.classList.toggle('active');
            button.style.background = 'transparent'; // Ensure background remains transparent on click

            // Update the icon
            if (details.classList.contains('active')) {
                icon.classList.remove('fa-chevron-down');
                icon.classList.add('fa-chevron-up');

                // Fetch and display dynamic data related to the project
                updateProjectInfo(projectId);
            } else {
                icon.classList.remove('fa-chevron-up');
                icon.classList.add('fa-chevron-down');
            }

            // Smooth scroll into view if details are opening
            if (details.classList.contains('active')) {
                setTimeout(() => {
                    details.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                }, 100);
            }
        }
    });

    function updateProjectInfo(projectId) {
        const modelsCount = document.getElementById(projectId + "_models_count");
        const seedsCount = document.getElementById(projectId + "_seeds_count");
        const snapshotsCount = document.getElementById(projectId + "_snapshots_count");
        const sourcesCount = document.getElementById(projectId + "_sources_count");
        const testsCount = document.getElementById(projectId + "_tests_count");

        getDbtProjectTaskCount(projectId)
            .then(data => {
            if (data) {
                modelsCount.style.display = "block";
                modelsCount.textContent = `Models: ${data.models_count}`;

                seedsCount.style.display = "block";
                seedsCount.textContent = `Seeds: ${data.seeds_count}`;

                snapshotsCount.style.display = "block";
                snapshotsCount.textContent = `Snapshot: ${data.snapshots_count}`;

                sourcesCount.style.display = "block";
                sourcesCount.textContent = `Sources: ${data.sources_count}`;

                testsCount.style.display = "block";
                testsCount.textContent = `Tests: ${data.tests_count}`;
            }
        })
            .catch(error => {
            console.error("Error handling info:", error);
            resultDiv.innerHTML = `<p style="color: red;">Failed to fetch Airflow info.</p>`;
        });

        // Airflow info
        const dagId = document.getElementById(projectId+"_dag_id");
        const dagScheduleInterval = document.getElementById(projectId+"_dag_schedule_interval");
        const dagTags = document.getElementById(projectId+"_dag_tags");
        getAirflowInfo(projectId)
            .then(data => {
            if (data) {
                dagId.innerHTML = `${data.dag_name}`;
                dagScheduleInterval.innerHTML = `${data.schedule_interval}`;
                dagTags.innerHTML = `${data.dag_tags}`;
            }
        })
            .catch(error => {
            console.error("Error handling info:", error);
            resultDiv.innerHTML = `<p style="color: red;">Failed to fetch Airflow info.</p>`;
        });

        // Airbyte info
        const airbyteConnectionId = document.getElementById(projectId + "_airbyte_connection_id");
        getAirbyteInfo(projectId)
            .then(data => {
            if (data) {
                airbyteConnectionId.innerHTML = `${data.connection_id}`;
            }
        })
            .catch(error => {
            console.error("Error handling info:", error);
            resultDiv.innerHTML = `<p style="color: red;">Failed to fetch Airflow info.</p>`;
        });

        // General project info
        const projectLevel = document.getElementById(projectId + "_project_level");
        const airbyteWorkspaceId = document.getElementById(projectId + "_airbyte_workspace_id");
        const dataQualityUrl = document.getElementById(projectId + "_data_quality_url");
        const dbtDocsUrl = document.getElementById(projectId + "_dbt_docs_url");
        const repoPath = document.getElementById(projectId + "_repo_path");

        getProjectInfo(projectId)
            .then(data => {
            if (data) {
                projectLevel.innerHTML = `${data.dbt_project_level}`;
                airbyteWorkspaceId.innerHTML = `${data.airbyte_workspace}`;
                dataQualityUrl.href = `${data.data_quality_url}`;
                dbtDocsUrl.href = `${data.dbt_docs_url}`;
                repoPath.href = `${data.repo_path}`;
            }
        })
            .catch(error => {
            console.error("Error handling info:", error);
            resultDiv.innerHTML = `<p style="color: red;">Failed to fetch project info.</p>`;
        });
    }

    // Options Menu Toggle Handler (delegated)
    grid.addEventListener('click', function(event) {
        if (event.target.classList.contains('options-trigger')) {
            event.stopPropagation();
            const menu = event.target.nextElementSibling;

            // Close all other open menus first
            document.querySelectorAll('.options-menu.active').forEach(m => {
                if (m !== menu) m.classList.remove('active');
            });

            // Toggle current menu
            menu.classList.toggle('active');
        }
    });

    grid.addEventListener('click', function(event) {
        if (event.target.classList.contains('option-item')) {
            event.stopPropagation();
            const action = event.target.dataset.action;
            const projectCard = event.target.closest('.project-card');
            const projectId = projectCard.dataset.projectId;

            handleProjectAction(action, projectId, projectCard);
        }
    });
});


// Handle different project actions
function handleProjectAction(action, projectId, card) {
    // Add loading state
    card.classList.add('loading');

    switch(action) {
        case 'archive':
            archiveProject(projectId, card);
            break;
        case 'remove-project':
            removeProject(projectId, card);
            break;
        case 'remove-data':
            removeProjectData(projectId, card);
            break;
        default:
            console.warn('Unknown action:', action);
            card.classList.remove('loading');
    }
}

// Helper function to show notifications
function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;

    document.body.appendChild(notification);

    setTimeout(() => {
        notification.classList.add('show');
        setTimeout(() => {
            notification.classList.remove('show');
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    }, 100);
}

// API interaction functions
const API_ENDPOINTS = {
    archive: '/api/v3/projects/{id}/archive',
    remove: '/api/v3/projects/{id}',
    status: '/api/v3/projects/{id}/status'
};

// Project Actions Implementation
async function archiveProject(projectId, card) {
    try {
        const response = await fetch(
            API_ENDPOINTS.archive.replace('{id}', projectId),
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-KEY': getApiKey() // Implement this based on your auth method
                }
            }
        );

        if (response.ok) {
            showNotification('Project archived successfully', 'success');
            // Optional: Add visual indication that project is archived
            card.classList.add('archived');
        } else {
            const error = await response.json();
            throw new Error(error.message || 'Failed to archive project');
        }
    } catch (error) {
        showNotification(error.message, 'error');
    } finally {
        card.classList.remove('loading');
    }
}

async function removeProject(projectId, card) {
    if (!confirm('Are you sure you want to remove this project? This action cannot be undone.')) {
        card.classList.remove('loading');
        return;
    }

    try {
        const response = await fetch(
            `${API_ENDPOINTS.remove.replace('{id}', projectId)}?delete_folder=true&delete_data=false`,
            {
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-KEY': getApiKey()
                }
            }
        );

        if (response.ok) {
            showNotification('Project removed successfully', 'success');
            // Animate and remove the card
            card.style.opacity = '0';
            card.style.height = card.offsetHeight + 'px';
            setTimeout(() => {
                card.style.height = '0';
                card.style.margin = '0';
                card.style.padding = '0';
                setTimeout(() => card.remove(), 300);
            }, 300);
        } else {
            const error = await response.json();
            throw new Error(error.message || 'Failed to remove project');
        }
    } catch (error) {
        showNotification(error.message, 'error');
    } finally {
        card.classList.remove('loading');
    }
}

async function removeProjectData(projectId, card) {
    if (!confirm('Are you sure you want to remove only the project data? This action cannot be undone.')) {
        card.classList.remove('loading');
        return;
    }

    try {
        const response = await fetch(
            `${API_ENDPOINTS.remove.replace('{id}', projectId)}?delete_folder=false&delete_data=true`,
            {
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-KEY': getApiKey()
                }
            }
        );

        if (response.ok) {
            showNotification('Project data removed successfully', 'success');
            // Update the status indicators
            updateProjectStatus(projectId, card);
        } else {
            const error = await response.json();
            throw new Error(error.message || 'Failed to remove project data');
        }
    } catch (error) {
        showNotification(error.message, 'error');
    } finally {
        card.classList.remove('loading');
    }
}

// Update project status after actions
async function updateProjectStatus(projectId, card) {
    try {
        const response = await fetch(
            API_ENDPOINTS.status.replace('{id}', projectId),
            {
                headers: {
                    'X-API-KEY': getApiKey()
                }
            }
        );

        if (response.ok) {
            const status = await response.json();
            updateStatusBadges(card, status);
        }
    } catch (error) {
        console.error('Failed to update status:', error);
    }
}

// Update status badges in the UI
function updateStatusBadges(card, status) {
    const badges = card.querySelectorAll('.badge');
    badges.forEach(badge => {
        const feature = badge.textContent.toLowerCase().replace(/\s+/g, '_');
        if (status[feature] === true || status[feature] === 'true') {
            badge.classList.add('active');
        } else {
            badge.classList.remove('active');
        }
    });
}


// Enhanced UI interactions and animations
class ProjectUI {
    static initializeAnimations() {
        // Add smooth reveal animations for project cards
        document.querySelectorAll('.project-card').forEach((card, index) => {
            card.style.opacity = '0';
            card.style.transform = 'translateY(20px)';
            setTimeout(() => {
                card.style.transition = 'all 0.3s ease';
                card.style.opacity = '1';
                card.style.transform = 'translateY(0)';
            }, index * 100);
        });
    }

    static createLoadingSpinner() {
        const spinner = document.createElement('div');
        spinner.className = 'spinner';
        spinner.innerHTML = `
            <div class="spinner-content">
                <i class="fas fa-circle-notch fa-spin"></i>
            </div>
        `;
        return spinner;
    }

    static showLoading(element) {
        const spinner = this.createLoadingSpinner();
        element.appendChild(spinner);
        element.classList.add('loading');
    }

    static hideLoading(element) {
        element.classList.remove('loading');
        const spinner = element.querySelector('.spinner');
        if (spinner) spinner.remove();
    }

    static createErrorDisplay(message) {
        const errorElement = document.createElement('div');
        errorElement.className = 'error-message';
        errorElement.innerHTML = `
            <i class="fas fa-exclamation-circle"></i>
            <span>${message}</span>
        `;
        return errorElement;
    }
}

// Enhanced error handling
class ProjectError extends Error {
    constructor(message, type = 'error') {
        super(message);
        this.type = type;
    }
}

// Project actions with retry mechanism
class ProjectActions {
    static async withRetry(action, maxRetries = 3) {
        let lastError;
        for (let i = 0; i < maxRetries; i++) {
            try {
                return await action();
            } catch (error) {
                lastError = error;
                if (i < maxRetries - 1) {
                    await new Promise(resolve => setTimeout(resolve, 1000 * (i + 1)));
                }
            }
        }
        throw lastError;
    }

    static attachEventListeners() {
        // Debounced search functionality
        const searchInput = document.querySelector('#projectSearch');
        if (searchInput) {
            let timeout;
            searchInput.addEventListener('input', (e) => {
                clearTimeout(timeout);
                timeout = setTimeout(() => {
                    this.filterProjects(e.target.value);
                }, 300);
            });
        }

        // Sortable columns
        document.querySelectorAll('.sortable').forEach(header => {
            header.addEventListener('click', () => {
                this.sortProjects(header.dataset.sort);
            });
        });
    }

    static filterProjects(searchTerm) {
        const projects = document.querySelectorAll('.project-card');
        const term = searchTerm.toLowerCase();

        projects.forEach(project => {
            const projectName = project.querySelector('.project-name').textContent.toLowerCase();
            const owner = project.querySelector('.metadata-item span').textContent.toLowerCase();

            if (projectName.includes(term) || owner.includes(term)) {
                project.style.display = '';
            } else {
                project.style.display = 'none';
            }
        });
    }

    static sortProjects(criterion) {
        const projectGrid = document.querySelector('.project-grid');
        const projects = Array.from(projectGrid.children);

        projects.sort((a, b) => {
            let valueA, valueB;

            switch(criterion) {
                case 'name':
                    valueA = a.querySelector('.project-name').textContent;
                    valueB = b.querySelector('.project-name').textContent;
                    return valueA.localeCompare(valueB);
                case 'date':
                    valueA = new Date(a.querySelector('[data-modified]').dataset.modified);
                    valueB = new Date(b.querySelector('[data-modified]').dataset.modified);
                    return valueB - valueA;
                default:
                    return 0;
            }
        });

        // Reappend sorted projects with animation
        projects.forEach((project, index) => {
            project.style.transition = 'none';
            project.style.opacity = '0';
            projectGrid.appendChild(project);

            setTimeout(() => {
                project.style.transition = 'opacity 0.3s ease';
                project.style.opacity = '1';
            }, index * 50);
        });
    }
}

// Initialize everything when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    ProjectUI.initializeAnimations();
    ProjectActions.attachEventListeners();

    // Add keyboard shortcuts
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            // Close all open menus and details
            document.querySelectorAll('.options-menu.active').forEach(menu => {
                menu.classList.remove('active');
            });
        }
    });

    // Add handler for params update button
    document.querySelectorAll('.config-btn[data-action="update-params"]').forEach(button => {
        button.addEventListener('click', async (e) => {
            const projectCard = e.currentTarget.closest('.project-card');
            const projectId = projectCard.dataset.projectId;
            await ProjectConfig.handleParamsUpdate(projectId);
        });
    });
});

// Copy to clipboard functionality
function addCopyToClipboard() {
    document.querySelectorAll('.info-value').forEach(element => {
        element.addEventListener('click', async function() {
            try {
                await navigator.clipboard.writeText(this.textContent.trim());
                showNotification('Copied to clipboard!', 'success');
            } catch (err) {
                showNotification('Failed to copy text', 'error');
            }
        });
    });
}

// Configuration Actions Handler
class ProjectConfig {
    static initializeConfigButtons() {
        document.querySelectorAll('.config-btn').forEach(button => {
            button.addEventListener('click', (e) => {
                const action = e.currentTarget.dataset.action;
                const projectCard = e.currentTarget.closest('.project-card');
                const projectId = projectCard.dataset.projectId;

                this.handleConfigAction(action, projectId, e.currentTarget);
            });
        });
    }

    static async handleConfigAction(action, projectId, button) {
        // Add loading state
        button.classList.add('loading');

        try {
            switch(action) {
                case 'rename':
                    await this.handleRename(projectId);
                    break;
                case 'change-owner':
                    await this.handleOwnerChange(projectId);
                    break;
                case 'refresh-schema':
                    await this.handleSchemaRefresh(projectId);
                    break;
                case 'update-platform':
                    await this.handlePlatformUpdate(projectId);
                    break;
                case 'update-params':
                    await this.handleParamsUpdate(projectId);
                    break;
            }
        } catch (error) {
            showNotification(error.message, 'error');
        } finally {
            button.classList.remove('loading');
        }
    }

    static async handleRename(projectId) {
        const syncLogo = document.getElementById(`${projectId}_logo_project_rename`);
        const syncSpinner = document.getElementById(`${projectId}_loading_project_rename`);

        const responseMessage = document.getElementById(`${projectId}_response_project_rename_message`);
        const buttonChange = document.getElementById(`${projectId}_button_project_rename`);

        if (buttonChange.style.display === 'block') {
            const newName = await this.promptForInput(
                'Rename Project',
                'Enter new project name:',
                'text'
            );

            if (!newName) return;

            // Validate project name
            if (!/^[a-zA-Z0-9_-]+$/.test(newName)) {
                throw new Error('Project name can only contain letters, numbers, underscores, and hyphens');
            }

            syncLogo.style.display = "none";
            syncSpinner.style.display = "block";

            fetch(`${dbt_init_api_link}/api/v3/projects/${projectId}/rename`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-KEY': getApiKey()
                },
                body: JSON.stringify({
                    new_project_name: newName
                })
            })    .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    return response.clone().text().then(errorText => {
                        console.error(`HTTP Error Response ${response.status}:`, errorText);
                        throw new Error(`Request failed with status ${response.status}`);
                    });
                }
            })
                .then(result => {
                console.log('sdsd')
                if (result['success']) {
                    const Href = original_href + '/-/tree/' + result['branch_name'];
                    responseMessage.innerHTML = 'Your request was successfully finished.' +
                    ' Please check your Data-Model Repository. Open branch - click: <a href=' + Href +'><b>Open</b> </a>';
                    buttonChange.style.display = 'none';
                    responseMessage.style.display = 'block';
                }
                else
                {
                    alert('Something not okay')
                    console.log('Error: ', result)
                }
            })
                .catch(error => {
                console.error('Error:', error);
                responseMessage.innerHTML = 'Your request failed due to incorrect form information. \n' +
                'Error: ' + error.message;
            })
                .finally(() => {
                syncLogo.style.display = "block";
                syncSpinner.style.display = "none";
            })
        }
    }

    static async handleOwnerChange(projectId) {

        const syncLogo = document.getElementById(`${projectId}_logo_owner_change`);
        const syncSpinner = document.getElementById(`${projectId}_loading_owner_change`);

        const responseMessage = document.getElementById(`${projectId}_response_owner_change_message`);
        const buttonOwnerChange = document.getElementById(`${projectId}_button_owner_change`);

        if (buttonOwnerChange.style.display === 'block') {
            const newOwner = await this.promptForInput(
                'Change Project Owner',
                'Enter new owner name:',
                'text'
            );

            if (!newOwner) return;

            syncLogo.style.display = "none";
            syncSpinner.style.display = "block";

            fetch(`${dbt_init_api_link}/api/v3/projects/${projectId}/owner`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-KEY': getApiKey()
                },
                body: JSON.stringify({
                    owner_name: newOwner
                })
            })    .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    return response.clone().text().then(errorText => {
                        console.error(`HTTP Error Response ${response.status}:`, errorText);
                        throw new Error(`Request failed with status ${response.status}`);
                    });
                }
            })
                .then(result => {
                if (result['success']) {
                    const Href = original_href + '/-/tree/' + result['branch_name'];
                    responseMessage.innerHTML = 'Your request was successfully finished.' +
                    ' Please check your Data-Model Repository. Open branch - click: <a href=' + Href +'><b>Open</b> </a>';
                    buttonOwnerChange.style.display = 'none';
                    responseMessage.style.display = 'block';
                }
                else
                {
                    alert('Something not okay')
                }
            })
                .catch(error => {
                console.error('Error:', error);
                responseMessage.innerHTML = 'Your request failed due to incorrect form information. \n' +
                'Error: ' + error.message;
            })
                .finally(() => {
                syncLogo.style.display = "block";
                syncSpinner.style.display = "none";
            })
        }
    }



    static async handleSchemaRefresh(projectId) {


        const syncLogog = document.getElementById(`${projectId}_logo_refresh_source`);
        const syncSpinner = document.getElementById(`${projectId}_loading_spinner_refresh_source`);

        const responseMessage = document.getElementById(`${projectId}_response_message`);
        const buttonRefreshSource = document.getElementById(`${projectId}_button_refresh_source`);


        var variables;
        const [projectData, airbyteData] = await Promise.all([
            getProjectVariables(projectId),
            getAirbyteInfo(projectId)
        ]);

        const [key, value] =  Object.entries(projectData)[0];
        variables = value;

        if (!variables['AIRBYTE_CONNECTION_ID']){
            alert('The DBT project doesn\'t have an Airbyte connection')
            return;
        }

        if (buttonRefreshSource.style.display === 'block') {
            if (!confirm('Are you sure you want to refresh the source schema? This may take a few minutes. The action will create a new Git branch with the changes')) {
                return;
            }
        }
        else {
            return;
        }

        const init_version = airbyteData['init_version']
        if (!init_version) {
            alert('Something happens. The init version is empty.')
            return;
        }

        syncLogog.style.display = "none";
        syncSpinner.style.display = "block";

        const branch_name = generateBranchName(5)
        const operator = variables['OPERATOR']
        let request_data;
        if (operator != 'gke') {
            request_data =
            {   "branch_name": branch_name,
                "dbt_project_name": projectId,
                "dbt_project_owner": variables['DAG_OWNER'],
                "project_level": variables['PROJECT_LEVEL'],
                "workload_platform": variables['PLATFORM'],
                "init_version": String(init_version),
                "airbyte_connection_id": variables['AIRBYTE_CONNECTION_ID'],
                "airbyte_workspace_id": variables['AIRBYTE_WORKSPACE_ID'],
                "reinit_project": true
            }
        }
        else{
            request_data =
            {   "cluster_name": variables['CLUSTER_NAME'],
                "cluster_zone": variables['CLUSTER_ZONE'],
                "cluster_node_count": variables['CLUSTER_NODE_COUNT'],
                "cluster_machine_type": variables['CLUSTER_MACHINE_TYPE'],
                "cluster_machine_disk_type": variables['CLUSTER_MACHINE_DISK_TYPE'],
                "network": variables['NETWORK'],
                "subnetwork": variables['SUBNETWORK'],
                "privatenodes_ip_range": variables['PRIVATENODES_IP_RANGE'],
                "services_secondary_range_name": variables['SERVICES_SECONDARY_RANGE_NAME'],
                "cluster_secondary_range_name": variables['CLUSTER_SECONDARY_RANGE_NAME'],
                "branch_name": branch_name,
                "dbt_project_name": projectId,
                "dbt_project_owner": variables['DAG_OWNER'],
                "project_level": variables['PROJECT_LEVEL'],
                "workload_platform": variables['PLATFORM'],
                "init_version": String(init_version),
                "airbyte_connection_id": variables['AIRBYTE_CONNECTION_ID'],
                "airbyte_workspace_id": variables['AIRBYTE_WORKSPACE_ID'],
                "reinit_project": true
            }
        }

        fetch(dbt_init_api_link + '/api/v3/' + operator, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-KEY': getApiKey()
            },
            body: JSON.stringify(request_data)
        })    .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                return response.clone().text().then(errorText => {
                    console.error(`HTTP Error Response ${response.status}:`, errorText);
                    throw new Error(`Request failed with status ${response.status}`);
                });
            }
        })
            .then(result => {
            if (result['success']) {
                const Href = original_href + '/-/tree/' + branch_name;
                responseMessage.innerHTML = 'Your request was successfully finished.' +
                ' Please check your Data-Model Repository. Open branch - click: <a href=' + Href +'><b>Open</b> </a>';
                buttonRefreshSource.style.display = 'none';
                responseMessage.style.display = 'block';
            }
            else
            {
                alert('No changes in source models')
            }
        })
            .catch(error => {
            console.error('Error:', error);
            responseMessage.innerHTML = 'Your request failed due to incorrect form information. \n' +
            'Error: ' + error.message;
        })
            .finally(() => {
            syncLogog.style.display = "block";
            syncSpinner.style.display = "none";
        })
    }

    static async handlePlatformUpdate(projectId) {
        // Open platform configuration modal
        const modal = this.createConfigModal('Platform Configuration');
        // ... Modal content and handling
    }

    static async handleParamsUpdate(projectId) {
        // Open parameters configuration modal
        const modal = this.createConfigModal('Project Parameters');
        // ... Modal content and handling
    }

    static async updateProjectVariable(projectId, key, value) {
        const response = await fetch(`/api/v3/projects/${projectId}/variables`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-KEY': getApiKey()
            },
            body: JSON.stringify({
                variables: { [key]: value },
                branch_name: `update_${key}_${Date.now()}`
            })
        });

        if (!response.ok) {
            throw new Error(`Failed to update ${key}`);
        }
    }

    static promptForInput(title, message, type = 'text') {
        return new Promise((resolve) => {
            const value = prompt(message);
            resolve(value);
        });
    }

    static createConfigModal(title) {
        // Implementation for modal creation
        // This would be expanded based on your modal requirements
    }

    static async handleParamsUpdate(projectId) {
        if (!window.paramsEditor) {
            window.paramsEditor = new ParamsEditor();
        }
        await window.paramsEditor.openEditor(projectId);
    }
    
}

async function getDbtProjectTaskCount(projectId) {

    const url = `${dbt_init_api_link}/api/v3/projects/${projectId}/tasks`;

    const headers = {
        "Accept": "application/json",
        "X-API-KEY": getApiKey()
    };

    const modelsSpinner = document.getElementById(`${projectId}_loading_spinner_models_count`);
    const modelsCount = document.getElementById(`${projectId}_models_count`);

    const seedsSpinner = document.getElementById(`${projectId}_loading_spinner_seeds_count`);
    const seedsCount = document.getElementById(`${projectId}_seeds_count`);

    const snapshotsSpinner = document.getElementById(`${projectId}_loading_spinner_snapshots_count`);
    const snapshotsCount = document.getElementById(`${projectId}_snapshots_count`);

    const sourcesSpinner = document.getElementById(`${projectId}_loading_spinner_sources_count`);
    const sourcesCount = document.getElementById(`${projectId}_sources_count`);

    const testsSpinner = document.getElementById(`${projectId}_loading_spinner_tests_count`);
    const testsCount = document.getElementById(`${projectId}_tests_count`);

    try {
        // Show the spinner before the API call
        modelsCount.style.display = "none";
        modelsSpinner.style.display = "block";

        seedsCount.style.display = "none";
        seedsSpinner.style.display = "block";

        snapshotsCount.style.display = "none";
        snapshotsSpinner.style.display = "block";

        sourcesCount.style.display = "none";
        sourcesSpinner.style.display = "block";

        testsCount.style.display = "none";
        testsSpinner.style.display = "block";

        const response = await fetch(url, { headers });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data.tasks;
    } catch (error) {
        if (error.name === "SyntaxError") {
            console.error(`JSON decoding error: ${error.message}`);
        } else {
            console.error(`Request error: ${error.message}`);
        }
    } finally {
        modelsSpinner.style.display = "none";
        seedsSpinner.style.display = "none";
        snapshotsSpinner.style.display = "none";
        sourcesSpinner.style.display = "none";
        testsSpinner.style.display = "none";
    }

    return {};
}

async function getAirflowInfo(projectId) {

    const url = `${dbt_init_api_link}/api/v3/projects/${projectId}/airflow`;
    const headers = {
        "Accept": "application/json",
        "X-API-KEY": getApiKey()
    };

    try {
        const response = await fetch(url, { headers });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        if (error.name === "SyntaxError") {
            console.error(`JSON decoding error: ${error.message}`);
        } else {
            console.error(`Request error: ${error.message}`);
        }
    }

    return {};
}

async function getAirbyteInfo(projectId) {

    const url = `${dbt_init_api_link}/api/v3/projects/${projectId}/airbyte`;
    const headers = {
        "Accept": "application/json",
        "X-API-KEY": getApiKey()
    };

    try {
        const response = await fetch(url, { headers });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        if (error.name === "SyntaxError") {
            console.error(`JSON decoding error: ${error.message}`);
        } else {
            console.error(`Request error: ${error.message}`);
        }
    }

    return {};
}

async function getProjectInfo(projectId) {

    const url = `${dbt_init_api_link}/api/v3/projects/${projectId}/info`;
    const headers = {
        "Accept": "application/json",
        "X-API-KEY": getApiKey()
    };

    try {
        const response = await fetch(url, { headers });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        if (error.name === "SyntaxError") {
            console.error(`JSON decoding error: ${error.message}`);
        } else {
            console.error(`Request error: ${error.message}`);
        }
    }

    return {};
}

async function getProjectVariables(projectId) {

    const url = `${dbt_init_api_link}/api/v3/projects/${projectId}/variables`;
    const headers = {
        "Accept": "application/json",
        "X-API-KEY": getApiKey()
    };

    try {
        const response = await fetch(url, { headers });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data['variables'];
    } catch (error) {
        if (error.name === "SyntaxError") {
            console.error(`JSON decoding error: ${error.message}`);
        } else {
            console.error(`Request error: ${error.message}`);
        }
    }

    return {};
}

async function getProjectProfiles(projectId) {
    const url = `${dbt_init_api_link}/api/v3/projects/${projectId}/profiles`;
    const headers = {
        "Accept": "application/json",
        "X-API-KEY": getApiKey()
    };

    try {
        const response = await fetch(url, { headers });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        if (error.name === "SyntaxError") {
            console.error(`JSON decoding error: ${error.message}`);
        } else {
            console.error(`Request error: ${error.message}`);
        }
    }

    return {};
}

function generateBranchName(length) {
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    const today = new Date();
    const formattedDate = `${today.getFullYear().toString().slice(2)}${(today.getMonth() + 1).toString().padStart(2, '0')}${today.getDate().toString().padStart(2, '0')}`; // YYMMDD format

    let branchName = 'DD_' + formattedDate + '_';
    for (let i = 0; i < length; i++) {
        const randomIndex = Math.floor(Math.random() * characters.length);
        branchName += characters.charAt(randomIndex);
    }

    return branchName;
}

async function getAirbyteDestinationType(destinationId) {

    if (!destinationId) {
        return null;
    }

    const url = `${airbyte_link}/api/v1/destinations/get`;
    const headers = { "Content-Type": "application/json" };
    const data = { destinationId };

    try {
        const response = await fetch(url, {
            method: "POST",
            headers,
            body: JSON.stringify(data),
        });

        if (response.ok) {
            const responseJson = await response.json();
            const destinationDefinitionId = responseJson.destinationDefinitionId || "";
            const dockerImageTag = getAirbyteDestinationImage(destinationDefinitionId);

            if (dockerImageTag) {
                const parsedDockerImageTag = version.parse(dockerImageTag);
                const lastV1DockerImageTag = version.parse('1.10.2'); // last possible version for v1 destination

                return parsedDockerImageTag > lastV1DockerImageTag ? 2 : 1;
            }
        }
    } catch (error) {
        console.error(`Error fetching destination type: ${error}`);
        return null;
    }

    return null;
}

// YAML Editor Modal Functionality - Airflow Params
class ParamsEditor {
    constructor() {
        this.modal = document.getElementById('paramsEditorModal');
        this.editor = document.getElementById('yamlEditor');
        this.projectName = document.getElementById('currentProjectName');
        this.validationStatus = document.getElementById('validationStatus');
        this.currentProject = null;

        // Buttons
        this.closeBtn = document.getElementById('closeParamsEditor');
        this.saveBtn = document.getElementById('saveParamsBtn');
        this.validateBtn = document.getElementById('validateParamsBtn');
        this.formatBtn = document.getElementById('formatBtn');
        this.copyBtn = document.getElementById('copyBtn');

        this.initializeEventListeners();
    }

    initializeEventListeners() {
        // Close button and click outside
        this.closeBtn.addEventListener('click', () => this.closeEditor());
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) {
                this.closeEditor();
            }
        });

        // Editor buttons
        this.saveBtn.addEventListener('click', () => this.saveChanges());
        this.validateBtn.addEventListener('click', () => this.validateYaml());
        this.formatBtn.addEventListener('click', () => this.formatYaml());
        this.copyBtn.addEventListener('click', () => this.copyToClipboard());

        // Escape key to close
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.modal.style.display === 'block') {
                this.closeEditor();
            }
        });
    }

    // async openEditor(projectId) {
    //     this.currentProject = projectId;
    //     this.projectName.textContent = projectId;
    //     this.modal.style.display = 'block';
        
    //     try {
    //         const data = await this.fetchProjectVariables(projectId);
    //         const formattedYaml = this.formatYamlString(data);
    //         this.editor.value = formattedYaml;
    //         this.originalContent = formattedYaml;
    //         this.buildParameterTree(data);
    //     } catch (error) {
    //         console.error('Failed to load project variables:', error);
    //         showNotification('Failed to load project variables', 'error');
    //     }
    // }

    async openEditor(projectId) {
        this.currentProject = projectId;
        this.projectName.textContent = projectId;
        this.modal.style.display = 'block';
        
        try {
            const data = await this.fetchProjectVariables(projectId);
            const formattedYaml = this.formatYamlString(data);
            this.editor.value = formattedYaml;
            this.originalContent = formattedYaml;
        } catch (error) {
            console.error('Failed to load project variables:', error);
            showNotification('Failed to load project variables', 'error');
        }
    }

    closeEditor() {
        if (this.hasUnsavedChanges()) {
            if (!confirm('You have unsaved changes. Are you sure you want to close?')) {
                return;
            }
        }
        this.modal.style.display = 'none';
        this.editor.value = '';
        this.currentProject = null;
    }

    async fetchProjectVariables(projectId) {
        const response = await fetch(`${dbt_init_api_link}/api/v3/projects/${projectId}/variables`, {
            headers: {
                'Accept': 'application/json',
                'X-API-KEY': getApiKey()
            }
        });

        if (!response.ok) {
            throw new Error('Failed to fetch project variables');
        }

        const data = await response.json();
        return data.variables;
    }

    hasUnsavedChanges() {
        // Compare current editor content with original content
        // This is a basic implementation - you might want to add more sophisticated comparison
        return this.editor.value !== this.originalContent;
    }

    formatYamlString(data) {
        try {
            return jsyaml.dump(data, {
                indent: 2,
                lineWidth: -1,
                noRefs: true
            });
        } catch (error) {
            console.error('Error formatting YAML:', error);
            return JSON.stringify(data, null, 2);
        }
    }

    validateYaml() {
        try {
            jsyaml.load(this.editor.value);
            this.validationStatus.textContent = 'YAML is valid';
            this.validationStatus.className = 'validation-status valid';
            return true;
        } catch (error) {
            this.validationStatus.textContent = `Invalid YAML: ${error.message}`;
            this.validationStatus.className = 'validation-status invalid';
            return false;
        }
    }

    formatYaml() {
        try {
            const parsed = jsyaml.load(this.editor.value);
            this.editor.value = this.formatYamlString(parsed);
            this.validationStatus.textContent = 'YAML formatted successfully';
            this.validationStatus.className = 'validation-status valid';
        } catch (error) {
            this.validationStatus.textContent = 'Cannot format invalid YAML';
            this.validationStatus.className = 'validation-status invalid';
        }
    }

    copyToClipboard() {
        this.editor.select();
        document.execCommand('copy');
        showNotification('YAML copied to clipboard', 'success');
    }

    // buildParameterTree(data) {
    //     const treeContainer = document.getElementById('paramTree');
    //     treeContainer.innerHTML = ''; // Clear existing content
        
    //     if (!data || typeof data !== 'object') {
    //         console.warn('Invalid data format:', data);
    //         return;
    //     }
        
    //     let project_name, variables;
        
    //     if (data.project_name && data.variables) {
    //         project_name = data.project_name;
    //         variables = data.variables;
    //     } else {
    //         project_name = Object.keys(data)[0];
    //         variables = data[project_name];
    //     }
        
    //     const projectGroup = document.createElement('div');
    //     projectGroup.className = 'param-group';
        
    //     const projectHeader = document.createElement('div');
    //     projectHeader.className = 'param-group-header';
    //     projectHeader.innerHTML = `
    //         <i class="fas fa-folder"></i>
    //         <span>${project_name}</span>
    //     `;
    //     projectGroup.appendChild(projectHeader);
        
    //     const projectContent = document.createElement('div');
    //     projectContent.className = 'param-group-content';
        
    //     Object.entries(variables).forEach(([paramKey, paramValue]) => {
    //         const paramItem = document.createElement('div');
    //         paramItem.className = 'param-item';
    //         paramItem.innerHTML = `
    //             <i class="fas fa-key param-icon"></i>
    //             <span class="param-name">${paramKey}</span>
    //             <input type="text" class="param-value" value="${paramValue}">
    //         `;
    //         projectContent.appendChild(paramItem);
    //     });
        
    //     projectGroup.appendChild(projectContent);
    //     treeContainer.appendChild(projectGroup);
    // }

    generateRepositoryBranchUrl(originalUrl, branchName) {
        try {
            // If no original URL, return null
            if (!originalUrl) {
                return null;
            }
    
            // Create a URL object
            const url = new URL(originalUrl);
            
            // Parse the pathname
            const pathParts = url.pathname.split('/').filter(Boolean);
            
            // Check if the URL already contains 'tree'
            const treeIndex = pathParts.indexOf('tree');
            
            if (treeIndex !== -1) {
                // If 'tree' exists, replace the branch name
                pathParts[treeIndex + 1] = branchName;
            } else {
                // If no 'tree', insert 'tree' and branch name
                // Insert after the last path segment
                pathParts.splice(-1, 0, 'tree', branchName);
            }
    
            // Reconstruct the URL pathname
            url.pathname = pathParts.join('/');
            
            // Add ref_type for GitLab if it's a GitLab URL
            if (url.hostname.includes('gitlab')) {
                url.searchParams.set('ref_type', 'heads');
            }
    
            return url.toString();
        } catch (error) {
            console.error('Error generating branch URL:', error);
            return null;
        }
    }
    
    async saveChanges() {
        if (!this.validateYaml()) {
            showNotification('Cannot save invalid YAML', 'error');
            return;
        }
        
        try {
            const updatedYaml = this.editor.value;
            const parsedYaml = jsyaml.load(updatedYaml);
        
            const branchName = `cicd_workflow_variables_${Date.now()}`;
        
            const requestPayload = {
                branch_name: branchName,
                project_name: this.currentProject,
                variables: parsedYaml
            };
        
            const response = await fetch(`/api/v3/projects/${this.currentProject}/variables`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-KEY': getApiKey()
                },
                body: JSON.stringify(requestPayload)
            });
        
            // Parse the response
            const result = await response.json();
        
            if (!response.ok) {
                throw new Error(result.message || 'Failed to save variables');
            }
        
            const responseMessage = document.getElementById('paramsEditorResponseMessage');
            const saveBtn = document.getElementById('saveParamsBtn');
            
            // Get the repository URL dynamically
            const repoPathElement = document.getElementById(`${this.currentProject}_repo_path`);
            const originalRepoUrl = repoPathElement ? repoPathElement.href : '';
            
            // Generate branch URL
            const branchUrl = this.generateRepositoryBranchUrl(originalRepoUrl, result.branch_name);
        
            // Update response message
            if (branchUrl) {
                responseMessage.innerHTML = `
                    Changes successfully saved. 
                    Branch created: <b>${result.branch_name}</b>. 
                    Please check your repository. 
                    Open branch - click: <a href="${branchUrl}" target="_blank"><b>Open Branch</b></a>
                `;
            } else {
                responseMessage.innerHTML = `
                    Changes successfully saved. 
                    Branch created: <b>${result.branch_name}</b>. 
                    Please check your repository.
                `;
            }
            
            responseMessage.style.display = 'block';
            
            // Hide save button
            saveBtn.style.display = 'none';
        
            this.originalContent = updatedYaml;
        } catch (error) {
            console.error('Error saving changes:', error);
            showNotification(`Failed to save changes: ${error.message}`, 'error');
        }
    }

}

// YAML Editor Modal Functionality - dbt Project Config Params
document.querySelectorAll('.config-btn[data-action="update-platform"]').forEach(button => {
    button.addEventListener('click', async (e) => {
        const projectCard = e.currentTarget.closest('.project-card');
        const projectId = projectCard.dataset.projectId;
        
        if (!window.profileEditor) {
            window.profileEditor = new ProfileEditor();
        }
        await window.profileEditor.openEditor(projectId);
    });
});

class ProfileEditor {
    constructor() {
        this.modal = document.getElementById('profileEditorModal');
        this.editor = document.getElementById('profileYamlEditor');
        this.projectName = document.getElementById('currentProfileProjectName');
        this.validationStatus = document.getElementById('profileValidationStatus');
        this.currentProject = null;

        // Buttons
        this.closeBtn = document.getElementById('closeProfileEditor');
        this.saveBtn = document.getElementById('saveProfileBtn');
        this.validateBtn = document.getElementById('validateProfileBtn');
        this.formatBtn = document.getElementById('formatProfileBtn');
        this.copyBtn = document.getElementById('copyProfileBtn');

        this.initializeEventListeners();
    }

    initializeEventListeners() {
        // Close button and click outside
        this.closeBtn.addEventListener('click', () => this.closeEditor());
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) {
                this.closeEditor();
            }
        });

        // Editor buttons
        this.saveBtn.addEventListener('click', () => this.saveChanges());
        this.validateBtn.addEventListener('click', () => this.validateYaml());
        this.formatBtn.addEventListener('click', () => this.formatYaml());
        this.copyBtn.addEventListener('click', () => this.copyToClipboard());

        // Escape key to close
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.modal.style.display === 'block') {
                this.closeEditor();
            }
        });
    }

    async openEditor(projectId) {
        this.currentProject = projectId;
        this.projectName.textContent = projectId;
        this.modal.style.display = 'block';
        
        try {
            const data = await getProjectProfiles(projectId);
            const formattedYaml = this.formatYamlString(data);
            this.editor.value = formattedYaml;
            this.originalContent = formattedYaml;
        } catch (error) {
            console.error('Failed to load project profiles:', error);
            showNotification('Failed to load project profiles', 'error');
        }
    }

    closeEditor() {
        if (this.hasUnsavedChanges()) {
            if (!confirm('You have unsaved changes. Are you sure you want to close?')) {
                return;
            }
        }
        this.modal.style.display = 'none';
        this.editor.value = '';
        this.currentProject = null;
    }

    hasUnsavedChanges() {
        return this.editor.value !== this.originalContent;
    }

    formatYamlString(data) {
        try {
            return jsyaml.dump(data, {
                indent: 2,
                lineWidth: -1,
                noRefs: true
            });
        } catch (error) {
            console.error('Error formatting YAML:', error);
            return JSON.stringify(data, null, 2);
        }
    }

    validateYaml() {
        try {
            jsyaml.load(this.editor.value);
            this.validationStatus.textContent = 'YAML is valid';
            this.validationStatus.className = 'validation-status valid';
            return true;
        } catch (error) {
            this.validationStatus.textContent = `Invalid YAML: ${error.message}`;
            this.validationStatus.className = 'validation-status invalid';
            return false;
        }
    }

    formatYaml() {
        try {
            const parsed = jsyaml.load(this.editor.value);
            this.editor.value = this.formatYamlString(parsed);
            this.validationStatus.textContent = 'YAML formatted successfully';
            this.validationStatus.className = 'validation-status valid';
        } catch (error) {
            this.validationStatus.textContent = 'Cannot format invalid YAML';
            this.validationStatus.className = 'validation-status invalid';
        }
    }

    copyToClipboard() {
        this.editor.select();
        document.execCommand('copy');
        showNotification('YAML copied to clipboard', 'success');
    }

    generateRepositoryBranchUrl(originalUrl, branchName) {
        try {
            if (!originalUrl) {
                return null;
            }
    
            const url = new URL(originalUrl);
            const pathParts = url.pathname.split('/').filter(Boolean);
            const treeIndex = pathParts.indexOf('tree');
            
            if (treeIndex !== -1) {
                pathParts[treeIndex + 1] = branchName;
            } else {
                pathParts.splice(-1, 0, 'tree', branchName);
            }
    
            url.pathname = pathParts.join('/');
            
            if (url.hostname.includes('gitlab')) {
                url.searchParams.set('ref_type', 'heads');
            }
    
            return url.toString();
        } catch (error) {
            console.error('Error generating branch URL:', error);
            return null;
        }
    }
    
    async saveChanges() {
        if (!this.validateYaml()) {
            showNotification('Cannot save invalid YAML', 'error');
            return;
        }
        
        try {
            const updatedYaml = this.editor.value;
            const parsedYaml = jsyaml.load(updatedYaml);
        
            const branchName = `dbt_project_profiles_config_${Date.now()}`;
        
            const requestPayload = {
                branch_name: branchName,
                project_name: this.currentProject,
                profiles: parsedYaml
            };
        
            const response = await fetch(`/api/v3/projects/${this.currentProject}/profiles`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-KEY': getApiKey()
                },
                body: JSON.stringify(requestPayload)
            });
        
            const result = await response.json();
        
            if (!response.ok) {
                throw new Error(result.message || 'Failed to save profiles');
            }
        
            const responseMessage = document.getElementById('profileEditorResponseMessage');
            const saveBtn = document.getElementById('saveProfileBtn');
            
            const repoPathElement = document.getElementById(`${this.currentProject}_repo_path`);
            const originalRepoUrl = repoPathElement ? repoPathElement.href : '';
            
            const branchUrl = this.generateRepositoryBranchUrl(originalRepoUrl, result.branch_name);
        
            if (branchUrl) {
                responseMessage.innerHTML = `
                    Changes successfully saved. 
                    Branch created: <b>${result.branch_name}</b>. 
                    Please check your repository. 
                    Open branch - click: <a href="${branchUrl}" target="_blank"><b>Open Branch</b></a>
                `;
            } else {
                responseMessage.innerHTML = `
                    Changes successfully saved. 
                    Branch created: <b>${result.branch_name}</b>. 
                    Please check your repository.
                `;
            }
            
            responseMessage.style.display = 'block';
            saveBtn.style.display = 'none';
        
            this.originalContent = updatedYaml;
        } catch (error) {
            console.error('Error saving changes:', error);
            showNotification(`Failed to save changes: ${error.message}`, 'error');
        }
    }
}

/// Refresh Projects Button Functionality
document.addEventListener('DOMContentLoaded', function() {
    const refreshBtn = document.getElementById('refreshProjectsBtn');
    
    if (refreshBtn) {
        refreshBtn.addEventListener('click', function() {
            refreshProjects();
        });
    }
});

// Function to refresh projects
async function refreshProjects() {
    const refreshBtn = document.getElementById('refreshProjectsBtn');
    
    // Early return if button is already in loading state
    if (refreshBtn.classList.contains('loading')) {
        return;
    }
    
    // Set button to loading state
    refreshBtn.classList.add('loading');
    refreshBtn.innerHTML = '<i class="fas fa-spinner"></i> Refreshing...';
    
    try {
        // Step 1: Call API to refresh repository
        const response = await fetch(`${dbt_init_api_link}/api/v3/repository/refresh`, {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'X-API-KEY': getApiKey()
            }
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.message || 'Failed to refresh repository');
        }
        
        // Step 2: Call the backend to handle cache clearing
        // Instead of trying to build the cache key here, let the backend handle it
        await clearCacheSimple();
        
        // Show success message
        showToast('Projects refreshed successfully! Reloading page...', 'success');
        
        // Give some time for the toast to be visible before reload
        setTimeout(() => {
            // Use this approach to force a fresh page load bypassing cache
            window.location.href = window.location.pathname + '?refresh=' + new Date().getTime();
        }, 1500);
        
    } catch (error) {
        console.error('Error refreshing projects:', error);
        showToast(`Failed to refresh projects: ${error.message}`, 'error');
        
        // Reset button state
        refreshBtn.classList.remove('loading');
        refreshBtn.innerHTML = '<i class="fas fa-sync-alt"></i> Refresh Projects';
    }
}

// Simplified function to clear cache
async function clearCacheSimple() {
    try {
        const response = await fetch('/dbt-management/cache/clear', {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({})
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ message: 'Failed to clear cache' }));
            throw new Error(errorData.message || 'Failed to clear cache');
        }
        
        return await response.json();
    } catch (error) {
        console.error('Error clearing cache:', error);
        throw error;
    }
}

// Toast notification functionality
function showToast(message, type = 'info') {
    const toast = document.getElementById('toastNotification');
    const toastMessage = document.getElementById('toastMessage');
    const toastIcon = document.getElementById('toastIcon');
    
    // Set message
    toastMessage.textContent = message;
    
    // Set appropriate icon and class based on type
    toast.className = 'toast-notification';
    toast.classList.add(type);
    
    switch (type) {
        case 'success':
            toastIcon.className = 'fas fa-check-circle';
            break;
        case 'error':
            toastIcon.className = 'fas fa-exclamation-circle';
            break;
        default:
            toastIcon.className = 'fas fa-info-circle';
    }
    
    // Show the toast
    toast.classList.add('visible');
    
    // Auto-hide after 5 seconds
    setTimeout(() => {
        closeToast();
    }, 5000);
}

function closeToast() {
    const toast = document.getElementById('toastNotification');
    toast.classList.remove('visible');
}