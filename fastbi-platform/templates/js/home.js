/*-- CUSTOM STATIC COMPONENTS RJG--*/    
/********************* Button to hide main menu (custom) ********************/
$("[dom-event='CollapseMenuPrincipal']").click(function(){
   var obj = $(this);   
   if (obj.attr("dom-status") == "false"){
      obj.attr("dom-status", "true");      
      $(".rj-layout").addClass("rj-full-screen-display");

   }else{      
      obj.attr("dom-status", "false");      
      $(".rj-layout").removeClass("rj-full-screen-display");       
   }    
})

/******************** Button to hide responsive main menu (custom) ********************/
$("[dom-event='ShowSectionResp']").click(function(){
   var id_section = $($(this).attr("dom-target"));

   $(".rj-full-section-movil").hide();
   id_section.fadeIn();
})

/******************** Dropdown for main menu lists (custom) ********************/
$("[dom-event='DropDownSublist']").click(function(){  
   var obj = $(this);   
   if (obj.attr("dom-status") == "false"){
      $(".rj-link").attr("dom-status", "false"); 
      obj.attr("dom-status", "true"); 
      $(".rj-sublist").slideUp("fast"); 
      obj.siblings(".rj-sublist").slideDown("fast");
   }else{
      $(".rj-link").attr("dom-status", "false");
      obj.siblings(".rj-sublist").slideUp("fast");
   }      
})


/*-- DYNAMIC COMPONENTS --*/
/******************** Toggle Fade Panels ********************/
$("[dom-event='rj-togglefade-panel']").click(function(){
   var obj = $(this); 
   if (obj.attr("dom-status") == "false"){  
       obj.attr("dom-status", "true");
       $(obj.attr("dom-target")).addClass("rj-fade");          
   }else{
       obj.attr("dom-status", "false");      
       $(obj.attr("dom-target")).removeClass("rj-fade");
   }    
 })

/******************** Accordion - Collapse ********************/
  $("[dom-event='rj-accordion']").click(function(){  
    var obj = $(this);
    var parent = obj.attr("dom-parent");   
    var target = obj.attr("dom-target");

   if (obj.attr("dom-status") == "false"){
    
      $(parent + " .rj-btn-link").attr("dom-status", "false");           
      
      obj.attr("dom-status", "true");
      
      $(parent + " .rj-card-body").slideUp("fast");
      
      $(target).slideDown("fast");
   
   }else{   
   
      $( parent + " .rj-btn-link").attr("dom-status", "false");     
      
      $(target).slideUp("fast");
   
   }      
})  

/******************** Dropdown ********************/
$("[dom-event='rj-dropdown']").click(function(){  
   var obj = $(this); 
   
   if(obj.attr("dom-status") == "false"){ 

      $(".rj-dropdown .rj-dropdown-menu").hide();
     
      $("[dom-event='rj-dropdown']").attr("dom-status", "false");      
     
      obj.attr("dom-status", "true").siblings(".rj-dropdown-menu").fadeIn("fast");            

   }else{  
      obj.attr("dom-status", "false").siblings(".rj-dropdown-menu").hide();
   }       
})  

/******************** Tabs ********************/
$("[data-event='rj-tab']").click(function(){  
   var obj = $(this); 

   obj.parent().parent().find(".rj-tab-link").removeClass("rj-active");
   obj.parent().parent().siblings(".rj-tab-body").find(".rj-tab-content").hide();
   
   obj.addClass("rj-active");
   $(obj.attr("href")).fadeIn();

   return false;
})  

/******************** Light-Dark ********************/
$(document).ready(function() {
    // Function to detect system color scheme preference
    function getSystemColorScheme() {
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }

    // Function to update light/dark mode in the database
    function updateLightDarkMode(mode) {
        $.ajax({
            type: 'POST',
            url: '/update_light_dark_mode',
            data: {
                user_id: user_id,
                light_dark_mode: mode
            },
            success: function(response) {
                console.log(response);
                // Refresh the page after successfully updating the mode
                location.reload();
            },
            error: function(xhr, status, error) {
                console.error(xhr.responseText);
            }
        });
    }

    // Function to apply theme
    function applyTheme(mode) {
        var colorButton = $("[dom-event='ChangeScreenColor']");
        var icon = colorButton.find("i:first");
        var text = colorButton.find("#theme-text");
        
        if (mode === "dark") {
            colorButton.attr("dom-mode", "dark");
            icon.removeClass("fa-moon").addClass("fa-sun");
            text.text("Light Theme");
            $("body").addClass("rj-dark-mode");
        } else {
            colorButton.attr("dom-mode", "light");
            icon.removeClass("fa-sun").addClass("fa-moon");
            text.text("Dark Theme");
            $("body").removeClass("rj-dark-mode");
        }
    }

    // Initialize theme based on system preference if no user preference exists
    if (!lightDarkMode) {
        lightDarkMode = getSystemColorScheme();
        applyTheme(lightDarkMode);
        updateLightDarkMode(lightDarkMode);
    } else {
        applyTheme(lightDarkMode);
    }

    // Listen for system theme changes
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', e => {
        if (!lightDarkMode) { // Only auto-switch if user hasn't manually set a preference
            const newMode = e.matches ? 'dark' : 'light';
            applyTheme(newMode);
            updateLightDarkMode(newMode);
        }
    });

    // Function to toggle light/dark mode
    $("[dom-event='ChangeScreenColor']").click(function() {
        var colorButton = $(this);
        var colorMode = colorButton.attr("dom-mode");
        
        if (colorMode === "light") {
            lightDarkMode = "dark";
        } else {
            lightDarkMode = "light";
        }
        
        applyTheme(lightDarkMode);
        updateLightDarkMode(lightDarkMode);
    });
});

/******************** Follow Button ********************/
$(document).ready(function() {

   // Function to update follow mode in the database
   function updateFollowMode(mode) {
       $.ajax({
           type: 'POST',
           url: '/update_follow_mode',
           data: {
               user_id: user_id, // Pass the user's ID here
               follow_mode: mode
           },
           success: function(response) {
               console.log(response);
           },
           error: function(xhr, status, error) {
               console.error(xhr.responseText);
           }
       });
   }

   // Function to toggle follow mode
   $("[dom-event='Follow']").click(function() {
       var followButton = $(this);
       var followMode = followButton.attr("dom-mode");
       var icon = followButton.find("i:first");

       if (followMode === "off") {
           followButton.attr("dom-mode", "on");
           icon.removeClass("fa-lock").addClass("fa-lock-open");
           updateFollowMode("on"); // Update follow mode in the database
           openLinksInNewWindow("on"); // Update link behavior
       } else {
           followButton.attr("dom-mode", "off");
            icon.removeClass("fa-lock-open").addClass("fa-lock");
           updateFollowMode("off"); // Update follow mode in the database
           openLinksInNewWindow("off"); // Update link behavior
       }
   });

    // Function to open href links in new windows if follow mode is enabled
    function openLinksInNewWindow(mode) {
        if (mode === "off") {
            $("a").removeAttr("target");
        } else {
            $("a").attr("target", "_blank");
        }

        // Remove target="_blank" attribute from links with specific IDs
        // $("#home, #init, #mnmgt, #stats, #airbyte, #ide, #airflow, #redata, #dbtdocs, #datahub, #vscodewiki, #cicdworkflows, #dbtdevwiki").find("a").removeAttr("target");

        // Always add target="_blank" attribute to links with specific IDs
        $("#bi, #grafana, #objstr, #git, #gbq, #gcp, #wiki").find("a").attr("target", "_blank");

        // Ensure error-text links always open in a new window
        $("#error-text").find("a").attr("target", "_blank");
    }

   // Initialize follow button and link behavior based on loaded value
   if (followMode === "on") {
       var followButton = $("[dom-event='Follow']");
       followButton.attr("dom-mode", "on");
       followButton.find("i:first").removeClass("fa-lock").addClass("fa-lock-open");
       openLinksInNewWindow("on");
   } else {
       var followButton = $("[dom-event='Follow']");
       followButton.attr("dom-mode", "off");
       followButton.find("i:first").removeClass("fa-lock-open").addClass("fa-lock");
       openLinksInNewWindow("off");
   }
});




/******************** Iframe Button ********************/

$(document).ready(function() {
   // Function to update iframe mode in the database
   function updateIframeMode(mode) {
       $.ajax({
           type: 'POST',
           url: '/update_iframe_mode',
           data: {
               user_id: user_id, // Pass the user's ID here
               iframe_mode: mode
           },
           success: function(response) {
               console.log(response);
           },
           error: function(xhr, status, error) {
               console.error(xhr.responseText);
           }
       });
   }

   // Function to toggle iframe mode
   $("[dom-event='Iframe_enabled']").click(function() {
       var iframeButton = $(this);
       var iframeMode = iframeButton.attr("dom-mode");
       var icon = iframeButton.find("i:first");
       var text = iframeButton.find("#iframe-mode-text");

       if (iframeMode === "disabled") {
           iframeButton.attr("dom-mode", "enabled");
           icon.removeClass("fa-expand").addClass("fa-compress");
           text.text("Platform Services");
           updateIframeMode("enabled"); // Update iframe mode in the database
           updateLinkHrefs(true); // Update href links with enabled mode
       } else {
           iframeButton.attr("dom-mode", "disabled");
           icon.removeClass("fa-compress").addClass("fa-expand");
           text.text("Embed Services");
           updateIframeMode("disabled"); // Update iframe mode in the database
           updateLinkHrefs(false); // Update href links with disabled mode
       }
   });

   // Function to update href links based on iframe mode
   function updateLinkHrefs(enabled) {
       $(".rj-link").each(function() {
           var defaultHref = $(this).data("default-href");
           var iframeHref = $(this).data("iframe-href");

           if (enabled && iframeHref) {
               $(this).attr("href", iframeHref);
           } else {
               $(this).attr("href", defaultHref);
           }
       });
   }

   // Initialize iframe button and link hrefs based on loaded value
   var iframeButton = $("[dom-event='Iframe_enabled']");
   iframeButton.attr("dom-mode", iframeMode);
   if (iframeMode === "enabled") {
       iframeButton.find("i:first").removeClass("fa-expand").addClass("fa-compress");
       iframeButton.find("#iframe-mode-text").text("Platform Services");
       updateLinkHrefs(true);
   } else {
       iframeButton.find("i:first").removeClass("fa-compress").addClass("fa-expand");
       iframeButton.find("#iframe-mode-text").text("Embed Services");
       updateLinkHrefs(false);
   }
});

/******************** Hide Config Button when Menu is hidden ********************/
$(document).ready(function() {
    $("#BtnCollapseMenuPrincipal").click(function() {
        $(".rj-floating-menu").toggleClass("rj-hidden");
    });
});

/*-- BACK-END COMPONENTES RJG--*/        
/******************** Button that parses the user attributes to the active user in the layout ********************/
function RenderDataUserLayout(e){
  var src_avatar =  $(e).find("img").attr("src");

  $("#ItemFaceDataActive img").attr("src", src_avatar);
  $("#CarouselListFaces").animate({scrollLeft: 0}, 100)

}

function OpenSectionAccordionAM(e){
   var obj = $(e);
   if (obj.attr("dom-status") == "false"){
      $(".rj-btn-add").fadeOut();
      obj.siblings(".rj-btn-add").fadeIn("fast")
   }else{
      obj.siblings(".rj-btn-add").fadeOut("fast")      
   }
}

function OpenListAM(e){
   $(e).parent().parent().parent().siblings(".rj-card-body").find(".rj-composer-am").fadeIn();
}


function AddItemAM(e, id_list_am){ 
   var item_color = $(e).parent().parent().find(".jscolor").css("background-color");
   var item_desc = $(e).parent().parent().find(".rj-form-control").val();
   var items_list = $(id_list_am + " .rj-item-am");   
  
   if(item_color == "rgb(255, 255, 255)"){
      alert("Selecciona un color")
   }else if(item_desc == "" || item_desc == " " || item_desc == "  " || item_desc == "   "){
      alert("El campo de texto esta vacio")
   }else{
      

      for(var a = 0; a < items_list.length; a++){
         let textarea = $(items_list[a]).find(".rj-form-control").val();  
         let span = $(items_list[a]).find("span").css("background-color");
         if(span == item_color || textarea == item_desc){
            alert("El color o la descripción ya existen")
            return false;
         }else{
         }
      }


      for(var b = 0; b < items_list.length; b++){
         contador = b+2;
         $(items_list[b]).attr("id", id_list_am.replace("#List", "Item")+contador );
      }                          
      


      $(id_list_am).prepend('<li id="' + id_list_am.replace("#List", "Item")+'1" class="rj-item-am">'
                              +'<div class="rj-color-am">'
                                 +'<span id="RowColorAm" style="background-color: ' + item_color + '"></span>'
                              +'</div>'
                              +'<div class="rj-desc-am">'
                                 +'<p>' + item_desc + '</p>'
                                 +'<textarea id="RowDescAm" class="rj-custom-textarea" placeholder="Edita esta descripción">' + item_desc + '</textarea>'
                              +'</div>'
                              +'<div class="rj-menu-options-am">'
                                 +'<button class="rj-btn-option-am rj-edit" onclick="EditItemAM(this)"><i class="fa fa-pencil"></i></button>'
                                 +'<button class="rj-btn-option-am rj-delete" onclick="DeleteItemAM(this)"><i class="fa fa-trash"></i></button>'                                                                        
                              +'</div>'
                           +'</li>');        
                           

   }
}


function EditItemAM(e){   
   $(e).parent().hide();
   $(e).parent().siblings(".rj-desc-am").find("p").hide();   
   $(e).parent().siblings(".rj-desc-am").addClass("rj-visible-section");
}

function DeleteItemAM(e){

   $(e).parent().parent().fadeOut("fast");   

   setTimeout(function(){
      $(e).parent().parent().remove();   
   }, 1000);
}

/******************** Notification Button ********************/


// Function to show the notification popup
function showNotificationPopup(message) {
    $('#notificationMessage').html(message);  // Set the HTML content of the notification message
    $('#notificationPopup').fadeIn();
}

// Function to hide the notification popup
$('#closeNotification').click(function() {
    $('#notificationPopup').fadeOut();
});

// Show message on click
$('#BtnPanelNotifications').click(function() {
    if (message) {
        showNotificationPopup(message);
    }
});


/******************** Button that adds a new item to the list of improvement actions for sellers********************/
const mediaQuery = matchMedia('(min-width: 767px)');
const changeSize = mql =>{
  if(mql.matches){
      $(".rj-full-section-movil").show();
   }
}

mediaQuery.addListener(changeSize);
changeSize(mediaQuery);

/******************** Close profile and notification panels when clicking outside ********************/
$(document).on('click', function(event) {
    var $target = $(event.target);

    // --- Profile Panel ---
    var $panelProfile = $('#PanelProfile');
    var $btnPanelProfile = $('#BtnPanelProfile');

    // Check if the panel is open and the click is outside of it and not on the toggle button
    if ($panelProfile.hasClass('rj-fade') && !$target.closest('#PanelProfile').length && !$target.closest('#BtnPanelProfile').length) {
        // Close the panel
        $panelProfile.removeClass('rj-fade');
        // Reset the button's state
        $btnPanelProfile.attr('dom-status', 'false');
    }

    // --- Notification Popup ---
    var $notificationPopup = $('#notificationPopup');
    var $btnNotifications = $('#BtnPanelNotifications');

    // Check if the popup is visible and the click is outside of it and not on the toggle button
    if ($notificationPopup.is(':visible') && !$target.closest('#notificationPopup').length && !$target.closest('#BtnPanelNotifications').length) {
        // Hide the popup
        $notificationPopup.fadeOut();
    }
});
