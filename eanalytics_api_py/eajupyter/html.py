from IPython.display import HTML as _HTML

def button_hide_cells(
    tagname_divclass : dict,
    force_hide = False
):
    """ Hide cells matching the given tags

        Button to hide specific cells, while displaying config cells to the end users.

        Parameters
        ----------
        tag_name_divclass : a dict as { "tagname" : "divclass" }, mandatory
            "tag_name": name of the tag to hide
            "divclass : "input", "output", "input_output"
    

        force_hide: bool, optional
            Set to True to hide matching cell when executing the function
            Default :False

    Returns
    -------
    HTML button to toggle cell visibility.
    """

    force_hide = 1 if force_hide else 0 # javascript compatibility
    allowed_divclass = ["input", "output", "input_output"]     

    for tag_name, divclass in tagname_divclass.items():
        if divclass not in allowed_divclass:
            raise ValueError(f"divclass={divclass} not allowed, use: {', '.join(allowed_divclass)}")
        
    html = _HTML(f'<script>\
        hide_code = true;\
        function code_toggle(fromForm=false) {{\
            if ( ( !fromForm || typeof(fromForm) === "function" ) && ( !{force_hide} ) ) {{\
                return 0;\
            }}\
            $( "div.input" ).each(function() {{\
                var divInput = this;\
                $( this ).find("span.cell-tag").each(function() {{\
                    var tagname = $( this ).text();\
                    if ( tagname in {tagname_divclass}  ) {{\
                        var divname = {tagname_divclass}[tagname]; \
                        if ( hide_code ) {{\
                            if ( divname === "input" || divname === "input_output" ) $( divInput ).hide();\
                            if ( divname === "output" || divname === "input_output" ) $( divInput ).next().hide();\
                        }}\
                        else {{\
                            if ( divname === "input" || divname === "input_output" ) $( divInput ).show() ;\
                            if ( divname === "output" || divname === "input_output" )$( divInput ).next().show();\
                        }}\
                    }}\
                }});\
            }});\
            hide_code = !hide_code\
        }}\
        \
        $( document ).ready(code_toggle);\
        </script>\
        <form action="javascript:code_toggle(fromForm=true)"><input type="submit" value="Code toggle"></form>'
    )

    return html