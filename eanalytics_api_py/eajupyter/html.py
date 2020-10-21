"""Interact with Jupyter Notebook HTML code"""

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
    HTML button to toggle cell visibility. To use with the "display" command
    """
    if not isinstance(tagname_divclass, dict):
        raise TypeError("tagname_divclass should be a dict")

    force_hide = 1 if force_hide else 0 # javascript compatibility
    allowed_divclass = ["input", "output", "input_output"]

    for divclass in tagname_divclass.values():
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
        <form action="javascript:code_toggle(fromForm=true)">\
            <input type="submit" value="Code tag visibility">\
        </form>'
    )

    return html

def button_hide_input_cells():
    """ Toggle visibility of all input cells (div.input)

    Parameters
    ----------
    None

    Returns
    -------
    HTML button to toggle cell visibility. To use with the "display" command
    """
    html = _HTML(
        '''
        <script>
            code_show=false;
            function code_toggle() {
                 if (code_show){
                    $('div.input').hide();
                 }
                 else {
                    $('div.input').show();
                 }
                 code_show = !code_show
            }
            $( document ).ready(code_toggle);
        </script>
        <form action="javascript:code_toggle()">
            <input type="submit" value="Toggle input cells visiblity">
        </form>
        '''
    )
    return html

def remove_class_output_scroll():
    """ Remove all output cells scrollbar by removing the output_scroll class

    Parameters
    ----------
    None

    Returns
    -------
    HTML code to use inside the "display" command
    """
    js = "<script>$('.output_scroll').removeClass('output_scroll')</script>"
    return _HTML(js)
