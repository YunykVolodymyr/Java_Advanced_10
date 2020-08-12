$(document).ready(function() {
	$('.leftmenutrigger').on('click', function(e) {
		$('.side-nav').toggleClass("open");
		e.preventDefault();
	});
});


$("div.navbar-collapse button.product-logout").click(function() {

	$.get("logout", function(data) {
		if (data !== '') {
			console.log(window.location.href);
			var urlContent = window.location.href.split('/');
			urlContent.pop();
			urlContent.push(data);
			var c = urlContent.join('/');
			console.log(c);
			window.location = c;
			
		}
	});

});

$(document).ready(function(){
	$.get("userRole", function(data){
		if(data != ''){
			userRole = data;
		}
	}).done(function(){
			
		if(userRole == 'ADMINISTRATOR'){
			$('#onlyForUser').hide();
		} else if(userRole == 'USER') {
			$('#onlyForAdministrator').hide();
		}
		
	});
});