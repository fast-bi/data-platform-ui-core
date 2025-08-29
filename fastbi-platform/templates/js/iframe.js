jQuery(document).ready(function($) {
    // $ is now an alias for jQuery within this function
    $('.float-nav').click(function() {
        $('.main-nav, .menu-btn').toggleClass('active');
        $('.menu-btn').focus(); // Focus on the menu button when .float-nav is clicked
    });
});

document.cookie = "LSOLH=value; SameSite=None; Secure";

document.cookie = "__Secure-3PSID=value; SameSite=None; Secure";