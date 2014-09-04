$(function() {
	var html = "";
	for ( var i = 0; i < presets.length; i++) {
		html += "<strong>" + presets[i].title + "</strong>, " + presets[i].description + "<br/>"
		html += '<input class="presetQueryBtn" alt="'+i+'" type="button" value="Execute">'
		html += '<input class="presetShowBtn" alt="'+i+'" type="button" value="Show"><br/><br/>'

	}
	$("#presetsArea").html(html);

	$(".presetQueryBtn").bind("click", function() {
		var preset = presets[this.alt];
		var query = preset.query;
		var queryType = preset.queryType;
		queryArchive(query, queryType);
	});

	$(".presetShowBtn").bind("click", function() {
		var preset = presets[this.alt];
		var tag = $('<div title="'+preset.title+'"></div>');
		var queryHtml = $('<div />').text(preset.query).html();
		tag.html(queryHtml).dialog({
			modal : true,
			buttons : {
				Ok : function() {
					$(this).dialog("close");
				}
			}
		}).dialog('open');
	});

	$("#queryBtn").bind("click", function() {
		var query = $('#queryText').val();
		var queryType = $('input[name=queryType]:checked').val();
		queryArchive(query, queryType);
	});
	
	//non HTML 5 browser fallback for placeholder
	if ( !("placeholder" in document.createElement("input")) ) {
		$("input[placeholder], textarea[placeholder]").each(function() {
			var val = $(this).attr("placeholder");
			if ( this.value == "" ) {
				this.value = val;
			}
			$(this).focus(function() {
				if ( this.value == val ) {
					this.value = "";
				}
			}).blur(function() {
				if ( $.trim(this.value) == "" ) {
					this.value = val;
				}
			})
		});

		// Clear default placeholder values on form submit
		$('form').submit(function() {
            $(this).find("input[placeholder], textarea[placeholder]").each(function() {
                if ( this.value == $(this).attr("placeholder") ) {
                    this.value = "";
                }
            });
        });
	}

});

function queryArchive(query, queryType) {
	if (query != "") {
		$("#results").html("");
		$("#loading-div").show();
		$.ajax({
			url : host+"archive",
			data : {
				query : query,
				queryType: queryType
			},
			context : document.body
		}).done(function(response) {
			var text = "";
			var errorClass = "";
			if (response.success) {
				text = JSON.stringify(response.data);
				//text = $.param(text);
			} else {
				text = response.message;
				errorClass = "error-msg"
			}
			$("#results").html('<textarea class="'+errorClass+'">' + text + "</textarea>");
			$("#loading-div").hide();
		});
	} else {
		alert("no query!!")
	}
};
