// Wait for DOM to be fully loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeFormHandlers();
    initializeTabHandling();
    setupFormValidation();
});

// Initialize all form handlers
function initializeFormHandlers() {
    const form = document.getElementById('dbt-form');
    if (!form) return;

    form.addEventListener('submit', function (e) {
        const selectedValues = choices.getValue(true);

        if (selectedValues.length === 0) {
            e.preventDefault();

            choices.input.element.focus();
            document.querySelector('.choices').classList.add('error');

            const errorMessage = document.getElementById('connection-error-message');
            if (errorMessage) {
                errorMessage.style.display = 'block';
            }
            return false
        }

        handleFormSubmit(e)
    });




    // Cancel button handler
    const cancelBtn = document.querySelector('.btn-secondary');
    if (cancelBtn) {
        cancelBtn.addEventListener('click', handleFormCancel);
    }

    // Initialize dynamic field updates
    setupDynamicFieldUpdates();
}

function toggleSectionVisibility(sectionId, isVisible) {
    const section = document.getElementById(sectionId);

    if (section) {
        if (isVisible) {
            section.classList.remove('hidden');

            // Reattach to the original parent at the beginning
            if (section.dataset.originalParent) {
                const originalParent = document.querySelector(section.dataset.originalParent);
                if (originalParent) {
                    originalParent.prepend(section);
                }
            }
        } else {
            section.classList.add('hidden');

            // Store the original parent element to reattach it later
            if (!section.dataset.originalParent) {
                section.dataset.originalParent = `#${section.parentNode.id}`;
            }
            document.body.appendChild(section); // Move out of the form
        }
    }
}



// Tab switching functionality
function initializeTabHandling() {
    const tabs = document.querySelectorAll('.tab');
    const tabContents = document.querySelectorAll('.tab-content');

    // Hide all specific sections during init page
    document.querySelectorAll('[id$="-specific-section"]').forEach(element => {
        toggleSectionVisibility(element.id, false);
    });

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const tabId = tab.id;
            switchTab(tabId, tabs, tabContents);
        });
    });
}

// Switch between tabs
function switchTab(tabId, tabs, tabContents) {
    // Hide all specific sections before tab switching
    document.querySelectorAll('[id$="-specific-section"]').forEach(element => {
        toggleSectionVisibility(element.id, false);
    });

    // Remove active class from all tabs and contents
    tabs.forEach(tab => tab.classList.remove('active'));
    tabContents.forEach(content => content.classList.remove('active'));

    // Add active class to selected tab and content
    const selectedTab = document.querySelector(`[id="${tabId}"]`);
    const dataTab=selectedTab.getAttribute("data-tab")
    const selectedContent = document.getElementById(`${dataTab}-content`);
    const cardDescription = document.querySelector(".card-description");

    if (selectedTab && selectedContent) {
        selectedTab.classList.add('active');
        selectedContent.classList.add('active');
        cardDescription.innerHTML = 'Configure your new DBT project settings with <b>' + selectedTab.textContent + '</b> operator'

        const specificContentSections = document.getElementById(`${tabId}-specific-section`);
        if(specificContentSections){
            toggleSectionVisibility(specificContentSections.id, true);
        }
    }
}

// Function to construct Git provider-specific URLs
function constructGitProviderUrl(provider, baseUrl, branchName) {
    switch (provider.toLowerCase()) {
        case 'gitlab':
            return `${baseUrl}/-/tree/${branchName}`;
        case 'github':
            return `${baseUrl}/tree/${branchName}`;
        case 'gitea':
            return `${baseUrl}/src/branch/${branchName}`;
        case 'bitbucket':
            return `${baseUrl}/src/${branchName}`;
        default:
            console.warn(`Unknown Git provider: ${provider}. Falling back to GitLab URL pattern.`);
            return `${baseUrl}/-/tree/${branchName}`;
    }
}

const platformFields = {
    bigquery: [
        'gcp_project_id',
        'gcp_project_region',
        'gcp_project_sa_email'
    ],
    snowflake: [
        'snowflake_db',
        'snowflake_role',
        'snowflake_schema',
        'snowflake_warehouse'
    ],
    redshift: [
        'redshift_schema',
        'redshift_database'
    ],
    fabric: [
        'fabric_schema',
        'fabric_database'
    ]
};

function cleanDataByPlatform(data) {
    const selectedPlatform = data.data_warehouse_platform;

    Object.entries(platformFields).forEach(([platform, fields]) => {
        if (platform !== selectedPlatform) {
            fields.forEach(field => {
                delete data[field];
            });
        }
    });
}

// Handle form submission
async function handleFormSubmit(event) {
    event.preventDefault();

    const submitBtn = event.target.querySelector('button[type="submit"]');
    const cancelBtn = event.target.querySelector('button[type="button"]');
    const btnText = submitBtn.querySelector('.button-text');
    const loader = submitBtn.querySelector('.loader');

    if (!validateForm(event.target)) {
        showNotification('error', 'Please fill in all required fields correctly.');
        return;
    }
    const responseMessage = document.getElementById('responseMessageShared');

    toggleSubmitButton(submitBtn, btnText, loader, true);

    // Gather form data
    const formData = new FormData(event.target);
    const data = Object.fromEntries(formData.entries());

    const form = document.getElementById('dbt-form');
    if (typeof data.airbyte_connection_id === 'string') {
        try {
            data.airbyte_connection_id = JSON.parse(data.airbyte_connection_id);
        } catch (e) {
            console.error('Failed to parse shared_airbyte_connection_id:', e);
            data.airbyte_connection_id = [];
        }
    } else {
        data.airbyte_connection_id = [];
    }

    form.querySelectorAll("input[type='checkbox']").forEach(checkbox => {
        data[checkbox.name] = checkbox.checked ? "True" : "False";
    });

    cleanDataByPlatform(data)

    const operator = document.querySelector('.tab.active').getAttribute('id');

    // log request data for troubleshooting purposes
    // console.log(data)
    // Make API call
    fetch(dbt_init_api_link + '/api/v3/' + operator, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-API-KEY': dbt_init_api_key
        },
        body: JSON.stringify(data)
    })    .then(response => {
        if (response.ok) {
            return response.json().then(json => {
                //console.log(json);
                return json;
            });
        } else {
            return response.clone().text().then(errorText => {
                console.error(`HTTP Error Response ${response.status}:`, errorText);
                throw new Error(`Request failed with status ${response.status}`);
            });
        }
    })

    // Modified form submission success handler
        .then(result => {

        if (!result.success) {
            console.error('API responded with success: false', result);
            throw new Error('The operation failed on the server side.');

        }

        const gitProvider = document.getElementById('git-provider').value;
        const Href = constructGitProviderUrl(gitProvider, original_href, data['branch_name']);

        responseMessage.innerHTML = 'Your request was successfully finished. ' +
        'Please check your Data-Model Repository. Open branch - click: ' +
        `<a href="${Href}" target="_blank"><b>Open</b></a>`;
        responseMessage.className = 'u-form-send-message u-form-send-success';

        event.target.reset();
    })

        .catch(error => {
        console.error('Error:', error);
        responseMessage.innerHTML = 'Your request failed due to incorrect form information. \n' +
        'Please review the form values and make sure they are entered correctly before trying again. \n' +
        'Error: ' + error.message;
        responseMessage.className = 'u-form-send-message u-form-send-error';
    })
        .finally(() => {
        // Re-enable button and hide loader
        toggleSubmitButton(submitBtn, btnText, loader, false);
        responseMessage.style.display = 'block';
        submitBtn.className = 'hidden';
        cancelBtn.className = 'hidden';

    });
}

// Toggle submit button state
function toggleSubmitButton(button, text, loader, isLoading) {
    button.disabled = isLoading;
    text.style.display = isLoading ? 'none' : 'block';
    loader.style.display = isLoading ? 'block' : 'none';
}

// Handle form cancellation
function handleFormCancel() {
    const form = document.getElementById('dbt-form');
    if (form) {
        form.reset();
        resetValidationStyles();
        const branchName = generateBranchName(5);
        document.getElementById('shared_branch_name').value = branchName;
    }
}

// Reset validation styles
function resetValidationStyles() {
    const inputs = document.querySelectorAll('.form-input');
    inputs.forEach(input => {
        input.style.borderColor = '';
        input.style.backgroundColor = '';
        const errorMessage = input.parentElement.querySelector('.error-message');
        if (errorMessage) {
            errorMessage.remove();
        }
    });
}

// Setup form validation
function setupFormValidation() {
    const form = document.getElementById('dbt-form');
    if (!form) return;

    const requiredInputs = form.querySelectorAll('input[required], select[required]');
    requiredInputs.forEach(input => {
        input.addEventListener('input', debounce(validateInput, 300));
        input.addEventListener('blur', validateInput);
    });
}

// Validate individual input
function validateInput(event) {
    const input = event.target;
    const isValid = input.checkValidity();
    const isDarkMode = document.body.classList.contains('rj-dark-mode');

    // Remove any existing error message
    const existingError = input.parentElement.querySelector('.error-message');
    if (existingError) {
        existingError.remove();
    }

    if (isValid) {
        input.style.borderColor = '#10b981';
        input.style.backgroundColor = isDarkMode ? '#3b413d' : '#f0fdf4';
    } else {
        input.style.borderColor = '#ef4444';
        input.style.backgroundColor = isDarkMode ? '#413d3d' : '#fef2f2';

        // Add error message
        const errorMessage = document.createElement('div');
        errorMessage.className = 'error-message';
        errorMessage.style.color = '#ef4444';
        errorMessage.style.fontSize = '0.75rem';
        errorMessage.style.marginTop = '0.25rem';
        errorMessage.textContent = input.validationMessage;
        input.parentElement.appendChild(errorMessage);
    }
}

// Validate entire form
function validateForm(form) {
    let isValid = true;
    const requiredInputs = form.querySelectorAll('input[required], select[required]');

    requiredInputs.forEach(input => {
        if (!input.checkValidity()) {
            isValid = false;
            const event = { target: input };
            validateInput(event);
        }
    });

    return isValid;
}

// Setup dynamic field updates
function setupDynamicFieldUpdates() {

    // Advanced settings toggle
    const advancedToggle = document.querySelector('[name="shared-advanced-settings"]');
    if (advancedToggle) {
        advancedToggle.addEventListener('change', toggleAdvancedSettings);
    }
}


// Toggle advanced settings
function toggleAdvancedSettings() {
    const sharedAdvancedProperties = document.getElementById("shared_advanced_properties");
    const isChecked = sharedAdvancedProperties.checked;
    const sharedAdvancedSettings = document.getElementById('shared-advanced-settings');

    if (sharedAdvancedSettings) {
        sharedAdvancedSettings.style.display = isChecked ? 'block' : 'none';
    }
}

// Toggle Data Catalog -> Basic BI Data-Model
function toggleDatahubEnabled() {

    const sharedDatahubEnabled = document.getElementById("shared-datahub-enabled");
    const isChecked = sharedDatahubEnabled.checked;
    const sharedDataAnalysisDbtProjectName = document.getElementById('shared-data-analysis-dbt-project-name');

    if (sharedDataAnalysisDbtProjectName) {
        sharedDataAnalysisDbtProjectName.style.display = isChecked ? 'block' : 'none';
    }
}


// Debounce helper function
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Add keyframes for notifications if they don't exist
if (!document.querySelector('#notification-keyframes')) {
    const styleSheet = document.createElement('style');
    styleSheet.id = 'notification-keyframes';
    styleSheet.textContent = `
        @keyframes slideIn {
            from { transform: translateX(100%); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }

        @keyframes slideOut {
            from { transform: translateX(0); opacity: 1; }
            to { transform: translateX(100%); opacity: 0; }
        }
    `;
    document.head.appendChild(styleSheet);
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

document.addEventListener('DOMContentLoaded', function () {
    const branchName = generateBranchName(5);
    document.getElementById('shared_branch_name').value = branchName; // Set the value for the first input field

});

const projectNameInput = document.getElementById("shared_project_name");

projectNameInput.addEventListener("input", () => {
    // Replace any character that is not a letter, number, or underscore
    projectNameInput.value = projectNameInput.value.replace(/[^a-zA-Z0-9_]/g, '');
});