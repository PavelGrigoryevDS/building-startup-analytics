import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "png" 
# pio.renderers.default = "vscode" 
# Settings for plotly
colorway_tableau = [
    '#4C78A8',
    '#FF9E4A',
    '#57B16E',
    '#E25559',
    '#8B6BB7',
    '#A17C6B',
    '#E377C2',
    '#7F7F7F',
    '#B5BD4E',
    '#5BB0D9'
]
custom_theme = go.layout.Template(
    layout=go.Layout(
        colorway=colorway_tableau,
    )
)
custom_template = pio.templates["plotly_white"].update(
    layout=dict(
        colorway=colorway_tableau
        , margin=dict(l=50, r=50, b=50, t=80)
        , xaxis_showline=True
        , yaxis_showline=True
        , xaxis_zeroline=False
        , yaxis_zeroline=False
        , xaxis_linecolor="rgba(0, 0, 0, 0.3)"
        , yaxis_linecolor="rgba(0, 0, 0, 0.3)"
        , xaxis_ticks="outside"
        , yaxis_ticks="outside"
        , legend_orientation='h'
        , legend_yanchor="top"
        , legend_y=1.12
        , legend_xanchor="center"
        , legend_x=0.5
        , legend_itemsizing="constant"
    )
)
pio.templates.default = custom_template
px.defaults.width = 600
px.defaults.height = 400
custom_template.data.histogram = [
    go.Histogram(marker=dict(line=dict(color='white', width=0.3)))
]
custom_template.data.scatter = [
    go.Scatter(marker=dict(line=dict(color='white', width=0.5)))
]
custom_template.data.scattergl = [
    go.Scattergl(marker=dict(line=dict(color='white', width=0.5))) 
]