from IPython.display import HTML

def hide_cells(tag=["code"]):
    """ Hide cells matching the given tags

        Useful to hide specific cells, while displaying config cells to the end users.


        Parameters
        ----------
        tag : str|list, obligatory
            A string "tag_name1"
            Or a list ["tag_name1", "tag_name2"]
            Default: ["code"]

    Returns
    -------
    HTML button to toggle cell visibility.
    """
    if type(tag) == str:
        tag = [ tag ]

    if type(tag) != list:
        raise TypeError(f"param tag type allowed=str|list but type={type(tag)} found")
        
    html = HTML(f'<script>\
        hide_code = true;\
        function code_toggle(fromForm=false) {{\
            if ( !fromForm || typeof(fromForm) === "function" ) {{\
                return 0;\
            }}\
            $( "div.input" ).each(function() {{\
                var divInput = this;\
                $( this ).find("span.cell-tag").each(function() {{\
                    if ( {tag}.includes( $( this ).text() ) && fromForm ) {{\
                        if ( hide_code ) {{\
                            $( divInput ).hide();\
                        }}\
                        else {{\
                            $( divInput ).show();\
                        }}\
                    }}\
                }});\
            }});\
            hide_code = !hide_code\
        }}\
        \
        $( document ).ready(code_toggle);\
        </script>\
        <form action="javascript:code_toggle(fromForm=true)"><input type="submit" value="Click here to toggle on/off the raw code."></form>'
    )

    return html